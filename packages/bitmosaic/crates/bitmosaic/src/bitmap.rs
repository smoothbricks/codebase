//! Format 2's Bitmap root (specs/bitmosaic/01-format.md §4): one control byte `0x80 | n` and `n` bitmap
//! bytes, bit `j` of byte `i` the member `8 i + j`, the last byte not zero. It is a bare little-endian bitset
//! behind one byte, read where it lies.
//!
//! [`BitmapView`] is the form's direct entry (§11): a caller that knows its bytes are a Bitmap pays no dispatch
//! on the control.

use crate::admission::{AdmissionClass, AdmissionError};
use crate::codec::select64;
use crate::range::Range;

/// The control's form bit: set on every Bitmap, clear on every other control.
pub(crate) const BITMAP_TAG: u8 = 0x80;
/// The greatest member the form holds: bit 7 of bitmap byte 126.
pub(crate) const BITMAP_LAST_MEMBER: u32 = 8 * 127 - 1;

/// A Bitmap image, borrowed. It holds the caller's slice from the control on, not only the image: the
/// image's extent comes from the control, and the bytes after it are the caller's own, so a word is one
/// load that is masked to the bitmap (§12.2: the caller says which bytes are readable by handing them).
#[derive(Clone, Copy, Debug)]
pub struct BitmapView<'a> {
    /// From the control to the end of what the caller handed over; at least `1 + n` bytes.
    image: &'a [u8],
    /// Bitmap bytes, from the control.
    n: usize,
}

impl<'a> BitmapView<'a> {
    /// Admit the Bitmap at the start of `image`, which may run past it: the control is a Bitmap's, its
    /// `n` bytes are present, and the last of them is not zero. The empty set, `80`, is admitted.
    #[inline]
    pub fn open(image: &'a [u8]) -> Result<Self, AdmissionError> {
        let Some(&control) = image.first() else {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "a Bitmap has no control byte",
            ));
        };
        if control & BITMAP_TAG == 0 {
            return Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "the control is not a Bitmap's",
            ));
        }
        let n = usize::from(control & !BITMAP_TAG);
        match image.get(n) {
            None => Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "a Bitmap is cut short of its bytes",
            )),
            Some(0) if n != 0 => Err(AdmissionError::new(
                AdmissionClass::Extent,
                n,
                "a Bitmap's last byte is zero",
            )),
            Some(_) => Ok(Self { image, n }),
        }
    }

    /// [`Self::open`] without the last-byte rule, for bytes an earlier pass admitted: the control and the
    /// extent are still checked, so no read can leave `image`.
    #[inline]
    pub fn open_admitted(image: &'a [u8]) -> Option<Self> {
        let control = *image.first()?;
        let n = usize::from(control & !BITMAP_TAG);
        (control & BITMAP_TAG != 0 && n < image.len()).then_some(Self { image, n })
    }

    /// The image's length in bytes: the control and the bitmap.
    #[inline]
    pub fn extent(&self) -> usize {
        1 + self.n
    }

    /// The bitmap bytes.
    #[inline]
    pub fn bytes(&self) -> &'a [u8] {
        self.image.get(1..=self.n).unwrap_or_default()
    }

    /// Whether the set has no member (the image is `80`, or an unverified bitmap of zero bytes).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The cardinality: the population count of the bitmap.
    #[inline]
    pub fn len(&self) -> u32 {
        (0..self.n.div_ceil(8))
            .map(|i| self.word(i).count_ones())
            .sum()
    }

    /// Little-endian word `i` of the bitmap: bit `j` is the member `64 i + j`. Bits past the bitmap are
    /// zero, and so is every word past it. Never reads past the image's extent (§12.2).
    #[inline]
    pub fn word(&self, i: usize) -> u64 {
        let Some(held) = self.n.checked_sub(8 * i).filter(|held| *held != 0) else {
            return 0;
        };
        let from = 1 + 8 * i;
        if held >= 8 {
            // SAFETY-by-bounds: `from + 8 <= 1 + self.n <= self.image.len()`.
            match self.image.get(from..from + 8) {
                Some(bytes) => u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])),
                None => 0,
            }
        } else {
            let mut word = [0u8; 8];
            let rest = self.image.get(from..from + held).unwrap_or_default();
            word[..rest.len()].copy_from_slice(rest);
            u64::from_le_bytes(word)
        }
    }

    /// Whether `v` is a member: one byte, a shift and a mask.
    #[inline]
    pub fn contains(&self, v: u32) -> bool {
        let at = (v >> 3) as usize;
        at < self.n
            && self
                .image
                .get(1 + at)
                .is_some_and(|byte| (byte >> (v & 7)) & 1 != 0)
    }

    /// Members strictly below `v`: the whole words below it by population count, then one word masked to
    /// the bits below it.
    #[inline]
    pub fn rank(&self, v: u32) -> u32 {
        let v = (v as usize).min(8 * self.n);
        let whole: u32 = (0..v / 64).map(|i| self.word(i).count_ones()).sum();
        whole + (self.word(v / 64) & ((1u64 << (v % 64)) - 1)).count_ones()
    }

    /// Members strictly below `v` iff `v` is present, in one pass (L0 byproduct).
    #[inline]
    pub fn rank_if_present(&self, v: u32) -> Option<u32> {
        let v_usize = v as usize;
        if v_usize >= 8 * self.n {
            return None;
        }
        let w_idx = v_usize / 64;
        let bit = v_usize % 64;
        let w = self.word(w_idx);
        if (w >> bit) & 1 == 0 {
            return None;
        }
        let whole: u32 = (0..w_idx).map(|i| self.word(i).count_ones()).sum();
        Some(whole + (w & ((1u64 << bit) - 1)).count_ones())
    }

    /// The `k`-th member ascending (0-indexed); `None` when `k >= len()`.
    ///
    /// Licensed by `specs/bitmosaic/laws/bitmap_probe.bend` (`select_skips_whole_words`,
    /// `select_stays_in_its_word`, `rank_of_select`, `select_is_a_member`).
    #[inline]
    pub fn select(&self, mut k: u32) -> Option<u32> {
        let words = self.n.div_ceil(8);
        for i in 0..words {
            let w = self.word(i);
            let ones = w.count_ones();
            if k < ones {
                return Some((i as u32) * 64 + select64(w, k));
            }
            k -= ones;
        }
        None
    }

    /// Least member at or above `v`; `None` when every member is below `v`.
    ///
    /// Equal to `select(rank(v))` (`specs/bitmosaic/laws/seek.bend`), answered directly by
    /// masking the starting word and scanning forward with `trailing_zeros`.
    #[inline]
    pub fn seek(&self, v: u32) -> Option<u32> {
        let v_usize = v as usize;
        if v_usize >= 8 * self.n {
            return None;
        }
        let words = self.n.div_ceil(8);
        let mut i = v_usize / 64;
        let first = self.word(i) & (u64::MAX << (v_usize % 64));
        if first != 0 {
            return Some((i as u32) * 64 + first.trailing_zeros());
        }
        i += 1;
        while i < words {
            let w = self.word(i);
            if w != 0 {
                return Some((i as u32) * 64 + w.trailing_zeros());
            }
            i += 1;
        }
        None
    }

    /// Cardinality of the intersection of two Bitmaps, word by word (`bitmap_probe.bend`).
    #[inline]
    pub fn and_len(&self, other: &BitmapView<'_>) -> u32 {
        let words = self.n.min(other.n).div_ceil(8);
        (0..words)
            .map(|i| (self.word(i) & other.word(i)).count_ones())
            .sum()
    }

    /// Members of the intersection of two Bitmaps strictly below `v`.
    #[inline]
    pub fn and_rank(&self, other: &BitmapView<'_>, v: u32) -> u32 {
        let limit = (v as usize).min(8 * self.n.min(other.n));
        let whole: u32 = (0..limit / 64)
            .map(|i| (self.word(i) & other.word(i)).count_ones())
            .sum();
        let tail = self.word(limit / 64) & other.word(limit / 64) & ((1u64 << (limit % 64)) - 1);
        whole + tail.count_ones()
    }

    /// Ascending internal walk over all members, allocation-free.
    #[inline]
    pub fn for_each(&self, mut f: impl FnMut(u32)) {
        let words = self.n.div_ceil(8);
        for i in 0..words {
            let mut w = self.word(i);
            let base = (i as u32) * 64;
            while w != 0 {
                f(base + w.trailing_zeros());
                w &= w - 1;
            }
        }
    }

    /// Ascending pull cursor implementing [`Range`] and [`Iterator`], with O(words) [`Range::seek`].
    #[inline]
    pub fn range(&self) -> BitmapRange<'a> {
        BitmapRange::new(*self)
    }

    /// The members, ascending.
    #[inline]
    pub fn iter(&self) -> BitmapIter<'a> {
        BitmapIter {
            view: *self,
            next: 0,
            word: 0,
            base: 0,
        }
    }

    /// Greatest member in the bitmap, or `None` when empty.
    #[inline]
    pub(crate) fn last_member(&self) -> Option<u32> {
        let words = self.n.div_ceil(8);
        for i in (0..words).rev() {
            let w = self.word(i);
            if w != 0 {
                return Some((i as u32) * 64 + (63 - w.leading_zeros()));
            }
        }
        None
    }

    /// Least member in the bitmap, or `None` when empty.
    #[inline]
    pub(crate) fn first_member(&self) -> Option<u32> {
        let words = self.n.div_ceil(8);
        for i in 0..words {
            let w = self.word(i);
            if w != 0 {
                return Some((i as u32) * 64 + w.trailing_zeros());
            }
        }
        None
    }
}

/// The members of a [`BitmapView`], ascending: per word, count the trailing zeros and clear the lowest bit.
#[derive(Clone, Debug)]
pub struct BitmapIter<'a> {
    view: BitmapView<'a>,
    /// The next word to load.
    next: usize,
    /// What is left of the current word.
    word: u64,
    /// The member of the current word's bit 0.
    base: u32,
}

impl Iterator for BitmapIter<'_> {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<u32> {
        while self.word == 0 {
            if 8 * self.next >= self.view.n {
                return None;
            }
            self.word = self.view.word(self.next);
            self.base = 64 * self.next as u32;
            self.next += 1;
        }
        let member = self.base + self.word.trailing_zeros();
        self.word &= self.word - 1;
        Some(member)
    }
}

/// Ascending [`Range`] cursor over a [`BitmapView`]. Copy, allocation-free, with idempotent `front`.
#[derive(Clone, Copy, Debug)]
pub struct BitmapRange<'a> {
    view: BitmapView<'a>,
    /// Next word index to load; `word` belongs to `next - 1`.
    next: usize,
    /// Unconsumed set bits of the current word; 0 iff exhausted.
    word: u64,
}

impl<'a> BitmapRange<'a> {
    #[inline]
    fn new(view: BitmapView<'a>) -> Self {
        let mut r = Self {
            view,
            next: 0,
            word: 0,
        };
        r.refill();
        r
    }

    #[inline]
    fn refill(&mut self) {
        let words = self.view.n.div_ceil(8);
        while self.word == 0 && self.next < words {
            self.word = self.view.word(self.next);
            self.next += 1;
        }
    }
}

impl Range for BitmapRange<'_> {
    type Item = u32;

    #[inline(always)]
    fn empty(&self) -> bool {
        self.word == 0
    }

    #[inline(always)]
    fn front(&self) -> u32 {
        debug_assert!(self.word != 0, "front on an empty BitmapRange");
        ((self.next - 1) as u32) * 64 + self.word.trailing_zeros()
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        debug_assert!(self.word != 0, "pop_front on an empty BitmapRange");
        self.word &= self.word - 1;
        if self.word == 0 {
            self.refill();
        }
    }

    #[inline]
    fn seek(&mut self, target: u32) {
        if self.word == 0 || self.front() >= target {
            return;
        }
        let t = target as usize;
        if t >= 8 * self.view.n {
            self.word = 0;
            self.next = self.view.n.div_ceil(8);
            return;
        }
        let goal = t / 64;
        let cur = self.next - 1;
        let mask = u64::MAX << (t % 64);
        if goal == cur {
            self.word &= mask;
        } else {
            self.word = self.view.word(goal) & mask;
            self.next = goal + 1;
        }
        if self.word == 0 {
            self.refill();
        }
    }
}

impl Iterator for BitmapRange<'_> {
    type Item = u32;

    #[inline(always)]
    fn next(&mut self) -> Option<u32> {
        if self.empty() {
            return None;
        }
        let v = self.front();
        self.pop_front();
        Some(v)
    }
}

/// The length of the Bitmap whose greatest member is `last`: `2 + last div 8`. The form ends at member
/// 1,015 (129 bytes would be asked for 1,016: [`bitmap_set`] and [`bitmap_seal`] refuse it).
#[inline]
pub const fn bitmap_len(last: u32) -> usize {
    2 + (last >> 3) as usize
}

/// Set member `v` in a Bitmap being written into `image`, whose bytes the caller zeroed: `image[0]` is
/// left for [`bitmap_seal`]. False, with nothing written, when `v` is past the form or its byte is past
/// `image`. It borrows `image` for this call alone, so a caller writes other bytes between calls.
#[inline]
pub fn bitmap_set(image: &mut [u8], v: u32) -> bool {
    if v > BITMAP_LAST_MEMBER {
        return false;
    }
    match image.get_mut(1 + (v >> 3) as usize) {
        Some(byte) => {
            *byte |= 1 << (v & 7);
            true
        }
        None => false,
    }
}

/// Write the control of the Bitmap whose greatest member is `last` and answer the image's length. None,
/// with nothing written, when `last` is past the form, the image is past `image`, or the byte of `last` is
/// zero: the control would then name a bitmap that ends in a zero byte, which no reader admits.
#[inline]
pub fn bitmap_seal(image: &mut [u8], last: u32) -> Option<usize> {
    if last > BITMAP_LAST_MEMBER {
        return None;
    }
    let n = 1 + (last >> 3) as usize;
    if *image.get(n)? == 0 {
        return None;
    }
    *image.first_mut()? = BITMAP_TAG | n as u8;
    Some(1 + n)
}

/// Write a complete Bitmap image for an ascending slice `members` directly into `out`, returning its
/// length (`1` for empty, `2 + last / 8` otherwise). Returns `None` if `members` contains a member past
/// `1015` or `out` is too short. Allocation-free.
#[inline]
pub fn write_bitmap(members: &[u32], out: &mut [u8]) -> Option<usize> {
    let Some(&last) = members.last() else {
        *out.first_mut()? = BITMAP_TAG;
        return Some(1);
    };
    if last > BITMAP_LAST_MEMBER {
        return None;
    }
    let len = bitmap_len(last);
    let buf = out.get_mut(..len)?;
    buf[0] = BITMAP_TAG | (len - 1) as u8;
    buf[1..].fill(0);
    for &v in members {
        buf[1 + (v >> 3) as usize] |= 1 << (v & 7);
    }
    Some(len)
}
