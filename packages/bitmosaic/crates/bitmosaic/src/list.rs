//! Format 2's Single and List roots (specs/bitmosaic/01-format.md §5):
//!
//! - **Single**: control `0x20 | (wa - 1)` followed by the member in `wa` bytes (`1..=4`, minimal width).
//! - **List**: control `((wd - 1) << 5) | ((k - 1) << 2) | (wa - 1)` with `k in 2..=8`, followed by
//!   `first` in `wa` bytes (`1..=4`, minimal width) and `k - 1` strictly ascending nonzero distances
//!   from `first`, each in `wd` bytes (`1..=4`, minimal width for the last distance).
//!
//! Both [`SingleView`] and [`ListView`] are direct entries (§11): zero-allocation borrowed views
//! that never read past their image extent (§12.2). [`ListView`] implements branchless compare-all
//! `contains`, `rank`, and `rank_if_present` licensed by `specs/bitmosaic/laws/list_probe.bend` and
//! `specs/bitmosaic/laws/one_image.bend` (`distances_rank_like_members`).

use crate::admission::{AdmissionClass, AdmissionError};
use crate::codec::{read_u16_at, read_u24_at, read_u32_at, read_uint_le, width_of, write_uint_le};
use crate::range::Range;

/// Minimum control byte for Single (`0x20..=0x23`).
pub(crate) const SINGLE_BASE: u8 = 0x20;
/// Maximum members a List holds.
pub(crate) const LIST_MAX_MEMBERS: usize = 8;

/// Encoded byte length of a Single image for `member`: `1 + W(member)` (`2..=5`).
#[inline(always)]
pub const fn single_len(member: u32) -> usize {
    1 + width_of(member)
}

/// Write a Single image for `member` into `out`, returning its byte length. Returns `None` if `out`
/// is shorter than [`single_len(member)`].
#[inline]
pub fn write_single(member: u32, out: &mut [u8]) -> Option<usize> {
    let wa = width_of(member);
    let len = 1 + wa;
    let buf = out.get_mut(..len)?;
    buf[0] = SINGLE_BASE | (wa as u8 - 1);
    write_uint_le(buf, 1, member, wa);
    Some(len)
}

/// Encoded byte length of a Single or List image for an ascending slice `members` (`1..=8` members):
/// `1 + W(first) + (k - 1) * W(last - first)`. Returns `None` when empty or `> 8` members.
#[inline(always)]
pub const fn list_len(members: &[u32]) -> Option<usize> {
    let k = members.len();
    if k == 0 || k > LIST_MAX_MEMBERS {
        return None;
    }
    let first = members[0];
    let last = members[k - 1];
    Some(1 + width_of(first) + (k - 1) * width_of(last - first))
}

/// Closed-form length for `k` (`1..=8`), `first`, and `span = last - first`.
#[inline(always)]
pub(crate) const fn list_len_from(k: usize, first: u32, span: u32) -> usize {
    1 + width_of(first) + (k - 1) * width_of(span)
}

/// Write a Single (for `k == 1`) or List (for `2..=8`) image for an ascending slice `members` into
/// `out`, returning its byte length. Returns `None` if `members` is empty, has `> 8` members, or
/// `out` is too short. Allocation-free.
#[inline]
pub fn write_list(members: &[u32], out: &mut [u8]) -> Option<usize> {
    let k = members.len();
    if k == 0 || k > LIST_MAX_MEMBERS {
        return None;
    }
    if k == 1 {
        return write_single(members[0], out);
    }
    let first = members[0];
    let span = members[k - 1] - first;
    let wa = width_of(first);
    let wd = width_of(span);
    let len = 1 + wa + (k - 1) * wd;
    let buf = out.get_mut(..len)?;
    buf[0] = ((wd as u8 - 1) << 5) | ((k as u8 - 1) << 2) | (wa as u8 - 1);
    write_uint_le(buf, 1, first, wa);
    let mut at = 1 + wa;
    for &v in &members[1..] {
        write_uint_le(buf, at, v - first, wd);
        at += wd;
    }
    Some(len)
}

// ── SingleView ───────────────────────────────────────────────────────────

/// Borrowed Format 2 Single image (§5): one member in `2..=5` bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SingleView<'a> {
    image: &'a [u8],
    value: u32,
    extent: usize,
}

impl<'a> SingleView<'a> {
    /// Admit a Single image at the start of `image`: control in `0x20..=0x23`, all `wa` bytes
    /// present, and top byte nonzero when `wa > 1` (minimal width rule, §5 & §11).
    #[inline]
    pub fn open(image: &'a [u8]) -> Result<Self, AdmissionError> {
        let Some(&control) = image.first() else {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "a Single has no control byte",
            ));
        };
        if !(SINGLE_BASE..=SINGLE_BASE + 3).contains(&control) {
            return Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "the control is not a Single's",
            ));
        }
        let wa = usize::from(control & 3) + 1;
        let extent = 1 + wa;
        if image.len() < extent {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "a Single is cut short of its member bytes",
            ));
        }
        if wa > 1 && image[wa] == 0 {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                wa,
                "a Single is in more bytes than its member needs",
            ));
        }
        let value = read_uint_le(image, 1, wa);
        Ok(Self {
            image: &image[..extent],
            value,
            extent,
        })
    }

    /// O(1) attach for an already-admitted Single image (skips the minimal-width check while
    /// keeping control and extent bounds).
    #[inline]
    pub fn open_admitted(image: &'a [u8]) -> Option<Self> {
        let control = *image.first()?;
        if !(SINGLE_BASE..=SINGLE_BASE + 3).contains(&control) {
            return None;
        }
        let wa = usize::from(control & 3) + 1;
        let extent = 1 + wa;
        let slice = image.get(..extent)?;
        let value = read_uint_le(slice, 1, wa);
        Some(Self {
            image: slice,
            value,
            extent,
        })
    }

    /// Image byte length (`2..=5`).
    #[inline(always)]
    pub fn extent(&self) -> usize {
        self.extent
    }

    /// Exact image bytes.
    #[inline(always)]
    pub fn bytes(&self) -> &'a [u8] {
        self.image
    }

    /// The sole member.
    #[inline(always)]
    pub fn value(&self) -> u32 {
        self.value
    }

    /// Always `1`.
    #[inline(always)]
    pub fn len(&self) -> u32 {
        1
    }

    /// Always `false`.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        false
    }

    #[inline(always)]
    pub fn contains(&self, v: u32) -> bool {
        v == self.value
    }

    #[inline(always)]
    pub fn rank(&self, v: u32) -> u32 {
        u32::from(self.value < v)
    }

    #[inline(always)]
    pub fn rank_if_present(&self, v: u32) -> Option<u32> {
        (v == self.value).then_some(0)
    }

    #[inline(always)]
    pub fn select(&self, k: u32) -> Option<u32> {
        (k == 0).then_some(self.value)
    }

    #[inline(always)]
    pub fn seek(&self, v: u32) -> Option<u32> {
        (self.value >= v).then_some(self.value)
    }

    #[inline(always)]
    pub fn for_each(&self, mut f: impl FnMut(u32)) {
        f(self.value);
    }

    #[inline(always)]
    pub fn range(&self) -> SingleRange {
        SingleRange {
            value: self.value,
            done: false,
        }
    }
}

/// Ascending [`Range`] cursor over a [`SingleView`].
#[derive(Clone, Copy, Debug)]
pub struct SingleRange {
    value: u32,
    done: bool,
}

impl Range for SingleRange {
    type Item = u32;

    #[inline(always)]
    fn empty(&self) -> bool {
        self.done
    }

    #[inline(always)]
    fn front(&self) -> u32 {
        debug_assert!(!self.done, "front on an empty SingleRange");
        self.value
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        debug_assert!(!self.done, "pop_front on an empty SingleRange");
        self.done = true;
    }

    #[inline(always)]
    fn seek(&mut self, target: u32) {
        if !self.done && self.value < target {
            self.done = true;
        }
    }
}

impl Iterator for SingleRange {
    type Item = u32;

    #[inline(always)]
    fn next(&mut self) -> Option<u32> {
        if self.done {
            return None;
        }
        self.done = true;
        Some(self.value)
    }
}

// ── ListView ─────────────────────────────────────────────────────────────

/// Borrowed Format 2 List image (§5): `2..=8` members, first member plus ascending distances.
///
/// Never reads a byte past `extent` (§12.2). Membership and rank compare every distance field
/// without data-dependent member branches (`specs/bitmosaic/laws/list_probe.bend`).
#[derive(Clone, Copy, Debug)]
pub struct ListView<'a> {
    /// Distance bytes only: `image[1 + wa .. extent]`, exact length `(k - 1) * wd`.
    dists: &'a [u8],
    /// The least member.
    first: u32,
    /// Total image length in bytes: `1 + wa + (k - 1) * wd`.
    extent: usize,
    /// Cardinality `k` in `2..=8`.
    k: u8,
    /// Distance field width `wd` in `1..=4`.
    wd: u8,
}

impl<'a> ListView<'a> {
    /// Admit a List image at the start of `image`: control in `0x04..=0x7F` with bits 2..4 nonzero,
    /// full extent present, minimal `wa` and `wd` widths, strictly ascending nonzero distances,
    /// and `first + last_distance <= u32::MAX` (§5 & §11).
    #[inline]
    pub fn open(image: &'a [u8]) -> Result<Self, AdmissionError> {
        let Some(&control) = image.first() else {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "a List has no control byte",
            ));
        };
        let k_minus_1 = (control >> 2) & 7;
        if control >= 0x80 || k_minus_1 == 0 {
            return Err(AdmissionError::new(
                AdmissionClass::Kind,
                0,
                "the control is not a List's",
            ));
        }
        let k = usize::from(k_minus_1) + 1;
        let wa = usize::from(control & 3) + 1;
        let wd = usize::from(control >> 5) + 1;
        let dists_at = 1 + wa;
        let extent = dists_at + (k - 1) * wd;
        if image.len() < extent {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                0,
                "a List is cut short of its fields",
            ));
        }
        if wa > 1 && image[wa] == 0 {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                wa,
                "a List's first member is in more bytes than it needs",
            ));
        }
        let first = read_uint_le(image, 1, wa);
        let dists = &image[dists_at..extent];
        let mut prev = 0u32;
        for i in 0..k - 1 {
            let d = read_uint_le(dists, i * wd, wd);
            if d <= prev {
                return Err(AdmissionError::new(
                    AdmissionClass::MapOrder,
                    dists_at + i * wd,
                    "a List's distances are not strictly ascending and nonzero",
                ));
            }
            prev = d;
        }
        if wd > 1 && image[extent - 1] == 0 {
            return Err(AdmissionError::new(
                AdmissionClass::Extent,
                extent - 1,
                "a List's distances are in more bytes than the last one needs",
            ));
        }
        if first.checked_add(prev).is_none() {
            return Err(AdmissionError::new(
                AdmissionClass::PlaneRange,
                dists_at + (k - 2) * wd,
                "a List's last member exceeds u32::MAX",
            ));
        }
        Ok(Self {
            dists,
            first,
            extent,
            k: k as u8,
            wd: wd as u8,
        })
    }

    /// O(1) attach for an already-admitted List image: checks control and extent bounds, skips
    /// minimal-width and ascension verification.
    #[inline]
    pub fn open_admitted(image: &'a [u8]) -> Option<Self> {
        let control = *image.first()?;
        let k_minus_1 = (control >> 2) & 7;
        if control >= 0x80 || k_minus_1 == 0 {
            return None;
        }
        let k = usize::from(k_minus_1) + 1;
        let wa = usize::from(control & 3) + 1;
        let wd = usize::from(control >> 5) + 1;
        let dists_at = 1 + wa;
        let extent = dists_at + (k - 1) * wd;
        let slice = image.get(..extent)?;
        let first = read_uint_le(slice, 1, wa);
        Some(Self {
            dists: &slice[dists_at..extent],
            first,
            extent,
            k: k as u8,
            wd: wd as u8,
        })
    }

    /// Image byte length (`3..=33`).
    #[inline(always)]
    pub fn extent(&self) -> usize {
        self.extent
    }

    /// Least member.
    #[inline(always)]
    pub fn first(&self) -> u32 {
        self.first
    }

    /// Greatest member.
    #[inline(always)]
    pub fn last(&self) -> u32 {
        self.first.wrapping_add(self.dist(self.k as usize - 2))
    }

    /// Span `last - first`.
    #[inline(always)]
    pub fn span(&self) -> u32 {
        self.dist(self.k as usize - 2)
    }

    /// Cardinality (`2..=8`).
    #[inline(always)]
    pub fn len(&self) -> u32 {
        u32::from(self.k)
    }

    /// Always `false`.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        false
    }

    #[inline(always)]
    fn dist(&self, idx: usize) -> u32 {
        let wd = usize::from(self.wd);
        read_uint_le(self.dists, idx * wd, wd)
    }

    /// Branchless compare-all membership probe (`list_probe.bend`: `or_of_equals_is_contains`).
    #[inline]
    pub fn contains(&self, v: u32) -> bool {
        if v <= self.first {
            return v == self.first;
        }
        let target = v - self.first;
        let m = self.k as usize - 1;
        match self.wd {
            1 => {
                if target > 0xFF {
                    return false;
                }
                let t = target as u8;
                let mut hit = false;
                for &b in self.dists {
                    hit |= b == t;
                }
                hit
            }
            2 => {
                if target > 0xFFFF {
                    return false;
                }
                let t = target as u16;
                let mut hit = false;
                for i in 0..m {
                    hit |= read_u16_at(self.dists, i * 2) == t;
                }
                hit
            }
            3 => {
                if target > 0xFF_FFFF {
                    return false;
                }
                let mut hit = false;
                for i in 0..m {
                    hit |= read_u24_at(self.dists, i * 3) == target;
                }
                hit
            }
            _ => {
                let mut hit = false;
                for i in 0..m {
                    hit |= read_u32_at(self.dists, i * 4) == target;
                }
                hit
            }
        }
    }

    /// Branchless compare-all rank (`list_probe.bend`: `count_below_is_rank`, `one_image.bend`:
    /// `distances_rank_like_members`).
    #[inline]
    pub fn rank(&self, v: u32) -> u32 {
        if v <= self.first {
            return 0;
        }
        let target = v - self.first;
        let m = self.k as usize - 1;
        let below = match self.wd {
            1 => {
                if target > 0xFF {
                    return u32::from(self.k);
                }
                let t = target as u8;
                let mut c = 0u32;
                for &b in self.dists {
                    c += u32::from(b < t);
                }
                c
            }
            2 => {
                if target > 0xFFFF {
                    return u32::from(self.k);
                }
                let t = target as u16;
                let mut c = 0u32;
                for i in 0..m {
                    c += u32::from(read_u16_at(self.dists, i * 2) < t);
                }
                c
            }
            3 => {
                if target > 0xFF_FFFF {
                    return u32::from(self.k);
                }
                let mut c = 0u32;
                for i in 0..m {
                    c += u32::from(read_u24_at(self.dists, i * 3) < target);
                }
                c
            }
            _ => {
                let mut c = 0u32;
                for i in 0..m {
                    c += u32::from(read_u32_at(self.dists, i * 4) < target);
                }
                c
            }
        };
        1 + below
    }

    /// Members strictly below `v` iff `v` is present, in one branchless compare-all pass.
    #[inline]
    pub fn rank_if_present(&self, v: u32) -> Option<u32> {
        if v < self.first {
            return None;
        }
        if v == self.first {
            return Some(0);
        }
        let target = v - self.first;
        let m = self.k as usize - 1;
        let (below, found) = match self.wd {
            1 => {
                if target > 0xFF {
                    return None;
                }
                let t = target as u8;
                let (mut c, mut hit) = (0u32, false);
                for &b in self.dists {
                    c += u32::from(b < t);
                    hit |= b == t;
                }
                (c, hit)
            }
            2 => {
                if target > 0xFFFF {
                    return None;
                }
                let t = target as u16;
                let (mut c, mut hit) = (0u32, false);
                for i in 0..m {
                    let d = read_u16_at(self.dists, i * 2);
                    c += u32::from(d < t);
                    hit |= d == t;
                }
                (c, hit)
            }
            3 => {
                if target > 0xFF_FFFF {
                    return None;
                }
                let (mut c, mut hit) = (0u32, false);
                for i in 0..m {
                    let d = read_u24_at(self.dists, i * 3);
                    c += u32::from(d < target);
                    hit |= d == target;
                }
                (c, hit)
            }
            _ => {
                let (mut c, mut hit) = (0u32, false);
                for i in 0..m {
                    let d = read_u32_at(self.dists, i * 4);
                    c += u32::from(d < target);
                    hit |= d == target;
                }
                (c, hit)
            }
        };
        found.then_some(1 + below)
    }

    /// `idx`-th member ascending (0-indexed); `None` when `idx >= len()`.
    #[inline]
    pub fn select(&self, idx: u32) -> Option<u32> {
        if idx >= u32::from(self.k) {
            return None;
        }
        if idx == 0 {
            return Some(self.first);
        }
        Some(self.first.wrapping_add(self.dist(idx as usize - 1)))
    }

    /// Least member at or above `v`; `None` when every member is below `v` (`seek.bend`).
    #[inline]
    pub fn seek(&self, v: u32) -> Option<u32> {
        let r = self.rank(v);
        self.select(r)
    }

    /// Ascending internal walk over all members, allocation-free.
    #[inline]
    pub fn for_each(&self, mut f: impl FnMut(u32)) {
        f(self.first);
        let m = self.k as usize - 1;
        for i in 0..m {
            f(self.first.wrapping_add(self.dist(i)));
        }
    }

    /// Ascending pull cursor implementing [`Range`] and [`Iterator`].
    #[inline]
    pub fn range(&self) -> ListRange<'a> {
        ListRange { view: *self, at: 0 }
    }

    /// Copy the at most 8 members into a stack buffer and return the occupied prefix.
    #[inline]
    pub(crate) fn copy_members<'b>(&self, buf: &'b mut [u32; LIST_MAX_MEMBERS]) -> &'b [u32] {
        let k = self.k as usize;
        buf[0] = self.first;
        for (i, slot) in buf[1..k].iter_mut().enumerate() {
            *slot = self.first.wrapping_add(self.dist(i));
        }
        &buf[..k]
    }
}

/// Ascending [`Range`] cursor over a [`ListView`].
#[derive(Clone, Copy, Debug)]
pub struct ListRange<'a> {
    view: ListView<'a>,
    at: u8,
}

impl Range for ListRange<'_> {
    type Item = u32;

    #[inline(always)]
    fn empty(&self) -> bool {
        self.at >= self.view.k
    }

    #[inline(always)]
    fn front(&self) -> u32 {
        debug_assert!(!self.empty(), "front on an empty ListRange");
        if self.at == 0 {
            self.view.first
        } else {
            self.view
                .first
                .wrapping_add(self.view.dist(self.at as usize - 1))
        }
    }

    #[inline(always)]
    fn pop_front(&mut self) {
        debug_assert!(!self.empty(), "pop_front on an empty ListRange");
        self.at += 1;
    }

    #[inline]
    fn seek(&mut self, target: u32) {
        if self.empty() || self.front() >= target {
            return;
        }
        let r = self.view.rank(target) as u8;
        if r > self.at {
            self.at = r;
        }
    }
}

impl Iterator for ListRange<'_> {
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
