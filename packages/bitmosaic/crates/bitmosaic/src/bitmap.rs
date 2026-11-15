//! Format 2's Bitmap root (specs/bitmosaic/01-format.md §4): one control byte `0x80 | n` and `n` bitmap
//! bytes, bit `j` of byte `i` the member `8 i + j`, the last byte not zero. It is a bare little-endian bitset
//! behind one byte, read where it lies.
//!
//! [`BitmapView`] is the form's direct entry (§11): a caller that knows its bytes are a Bitmap pays no dispatch
//! on the control. [`Image::open`] is the entry for a caller that does not: it tells the format by the first
//! byte (§3, §10) and names every root of format 2 this version of the crate does not read.

use crate::admission::{AdmissionClass, AdmissionError};
use crate::wire::{Bitmosaic64View, BitmosaicView};

/// The control's form bit: set on every Bitmap, clear on every other control.
const BITMAP: u8 = 0x80;
/// The greatest member the form holds: bit 7 of bitmap byte 126.
const LAST_MEMBER: u32 = 8 * 127 - 1;

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
        if control & BITMAP == 0 {
            return Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "the control is not a Bitmap's",
            ));
        }
        let n = usize::from(control & !BITMAP);
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
        let n = usize::from(control & !BITMAP);
        (control & BITMAP != 0 && n < image.len()).then_some(Self { image, n })
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
    /// zero, and so is every word past it.
    #[inline]
    pub fn word(&self, i: usize) -> u64 {
        let Some(held) = self.n.checked_sub(8 * i).filter(|held| *held != 0) else {
            return 0;
        };
        let from = 1 + 8 * i;
        let loaded = match self.image.get(from..from + 8) {
            Some(bytes) => u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])),
            None => {
                let mut word = [0u8; 8];
                let rest = self.image.get(from..).unwrap_or_default();
                for (to, byte) in word.iter_mut().zip(rest) {
                    *to = *byte;
                }
                u64::from_le_bytes(word)
            }
        };
        if held >= 8 {
            loaded
        } else {
            loaded & ((1u64 << (8 * held)) - 1)
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
    if v > LAST_MEMBER {
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
    if last > LAST_MEMBER {
        return None;
    }
    let n = 1 + (last >> 3) as usize;
    if *image.get(n)? == 0 {
        return None;
    }
    *image.first_mut()? = BITMAP | n as u8;
    Some(1 + n)
}

/// An image, by what its first byte says it is (§3 and §10).
#[derive(Clone, Copy, Debug)]
pub enum Image<'a> {
    /// Format 2, the Bitmap root.
    Bitmap(BitmapView<'a>),
    /// Format 1 (`42 4D 53`), a `u32` set, verified.
    Format1(BitmosaicView<'a>),
    /// Format 1, a `u64` set, verified.
    Format1Wide(Bitmosaic64View<'a>),
}

impl<'a> Image<'a> {
    /// Admit the image at the start of `bytes` whatever its format. A format-1 image is admitted as
    /// `open_verified` admits it. Of format 2 this version of the crate reads the Bitmap root; every other
    /// root is refused by name, as `kind` at the control.
    pub fn open(bytes: &'a [u8]) -> Result<Self, AdmissionError> {
        let refuse = |what| Err(AdmissionError::new(AdmissionClass::Kind, 0, what));
        match bytes {
            [] => Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "an image has no control byte",
            )),
            [control, ..] if control & BITMAP != 0 => BitmapView::open(bytes).map(Self::Bitmap),
            [0x42, 0x4D, 0x53, control, ..] if control & 1 != 0 => {
                Bitmosaic64View::open_verified(bytes).map(Self::Format1Wide)
            }
            [0x42, ..] => BitmosaicView::open_verified(bytes).map(Self::Format1),
            [0x00, ..] => refuse("the control 00 is no image"),
            [0x01, ..] => {
                refuse("format 2's u32 directory is not read by this version of the crate")
            }
            [0x02, ..] => {
                refuse("format 2's Elias-Fano root is not read by this version of the crate")
            }
            [0x03, ..] => {
                refuse("format 2's u64 directory is not read by this version of the crate")
            }
            [0x20..=0x23, ..] => {
                refuse("format 2's Single root is not read by this version of the crate")
            }
            [0x40..=0x43, ..] => refuse("the control is kept for a later format"),
            [0x60..=0x63, ..] => refuse("the control is unassigned"),
            [_, ..] => refuse("format 2's List root is not read by this version of the crate"),
        }
    }
}
