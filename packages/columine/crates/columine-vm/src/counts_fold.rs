//! Counts fold: one sample folded into counters inside an opaque frame field
//! of a struct-map row, in batch order.
//!
//! The instruction names byte positions inside a frame — one `u32` sample
//! count, one or more `u16` histograms, zero or more `u8` OR-masks — and the
//! frame's leading bytes. The count and the masks sit at constant offsets; a
//! histogram is a `u16` element run reached through a constant-offset `u32`
//! word (the run's region holds a `u32` element count, then the elements at
//! `elems_at`). Each element carries, beside its buckets and mask bytes, an
//! init frame: the zero-count frame its row starts from, holding whatever
//! else the row records (metadata the fold never interprets).
//!
//! This module knows no schema. The bytes it is handed are opaque, and what
//! it proves is physical only:
//!
//! - the constant ranges (count, region words, masks) lie at or past
//!   `origin` and do not overlap;
//! - every frame it reads — init or stored — leads with the instruction's
//!   prefix and reaches every constant range; each histogram's region lies
//!   inside the frame, records exactly `bin_count` elements, and overlaps
//!   neither a constant range nor another histogram;
//! - an init frame holds zero in the count, every bin and every mask;
//! - a stored frame equals the element's init frame outside the count, the
//!   bins and the masks, so a row keeps the metadata it was created with and
//!   an element that disagrees is refused rather than mixed in.
//!
//! None of that says the positions ARE a count, histograms and masks of the
//! frame's schema: equal leading bytes and in-bound ranges certify geometry,
//! not meaning. That certification belongs to whoever admits the program —
//! once, against the schema's public descriptor, before the VM runs it — and
//! this module must not be read as performing it.
//!
//! One sample adds one to the count, one to one bin of each histogram, and
//! ORs one byte into each mask. A bin about to pass `u16::MAX` halves its
//! whole histogram (floor) first; a count at `u32::MAX` refuses the sample.
//! Only count, element and mask bytes change — no length, region word or
//! element count. Every refusal is decided by [`CountsFoldOp::prepare`],
//! which only borrows, so a refused sample has mutated nothing;
//! [`CountsFoldOp::apply`] then writes the planned values into the row's
//! frame — the stored one in place, or a copy of the init frame for an
//! absent row. The caller decodes the instruction once per batch, owns where
//! the frame lives, and journals the write for undo, delta and fork.
//!
//! Operands, after the opcode byte:
//!
//! ```text
//! slot:index, key_col:index, field_idx:u8,
//! init_offsets_col:index, init_values_col:index,
//! count_off:index,
//! num_hists:u8, (ref_off:index, elems_at:u8, bin_count:u8, bucket_col:index) × num_hists,
//! num_masks:u8, (mask_off:index, mask_col:index) × num_masks,
//! prefix_len:index, prefix:u8[prefix_len]
//! ```
//!
//! `origin` is `prefix_len` (non-zero). `count_off`, `ref_off` and `mask_off`
//! are relative to it; a region word is itself relative to it, and
//! `elems_at` to the region. Bucket and mask columns are `u32` cells; the init
//! frame of element `e` is `values[offsets[e]..offsets[e + 1]]`, the offsets
//! being `u32` cells.

use columine_types::operand::Operands;

use crate::bytes;
use crate::element_body::OperandRead;
use columine_types::vo::{COUNTS_FOLD_CAPACITIES, VoCountsLayout};

pub(crate) const MAX_COUNTS_HISTOGRAMS: usize = COUNTS_FOLD_CAPACITIES.histograms.max;
pub(crate) const MAX_COUNTS_MASKS: usize = COUNTS_FOLD_CAPACITIES.masks.max;

const COUNT_WIDTH: u32 = 4;
const REF_WIDTH: u32 = 4;
const BIN_WIDTH: u32 = 2;
const MASK_WIDTH: u32 = 1;
const MAX_FIXED: usize = 1 + MAX_COUNTS_HISTOGRAMS + MAX_COUNTS_MASKS;
/// Counter ranges of one frame: the count, each histogram's elements, each mask.
const MAX_COUNTERS: usize = 1 + MAX_COUNTS_HISTOGRAMS + MAX_COUNTS_MASKS;

/// Why a counts-fold instruction or one of its samples was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CountsFoldError {
    /// Malformed operands, or constant ranges that overlap.
    InvalidProgram,
    /// A sample column cannot cover the element.
    ColumnUnderrun,
    /// A bucket past its histogram, a mask wider than one byte, or an init
    /// frame whose offsets run backwards, that does not hold the layout, or
    /// that holds a nonzero counter.
    InvalidCellValue,
    /// The stored frame lacks the prefix or does not hold the layout.
    FrameMismatch,
    /// The stored frame and the element's init frame differ outside the counters.
    MetadataConflict,
    /// The sample count is at `u32::MAX`; one more sample cannot be counted.
    CountOverflow,
}

/// One histogram: a `u16` run of `bin_count` elements behind the region word
/// at `ref_off`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CountsHistogram {
    /// Frame-absolute offset of the region word.
    pub ref_off: u32,
    /// Offset of element zero from the region start.
    pub elems_at: u8,
    pub bin_count: u8,
    pub bucket_col: u32,
}

/// One OR-mask byte.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CountsMask {
    /// Frame-absolute offset of the mask byte.
    pub off: u32,
    pub mask_col: u32,
}

/// A decoded, validated counts-fold instruction. Offsets are frame-absolute.
/// Not `Copy`: decode it once per batch and borrow it per element.
#[derive(Debug)]
pub struct CountsFoldOp<'p> {
    pub slot: u32,
    pub key_col: u32,
    pub field_idx: u8,
    pub init_offsets_col: u32,
    pub init_values_col: u32,
    count_off: u32,
    hists: [CountsHistogram; MAX_COUNTS_HISTOGRAMS],
    num_hists: u8,
    masks: [CountsMask; MAX_COUNTS_MASKS],
    num_masks: u8,
    /// The constant ranges, `[start, end)`: count, region words, masks.
    fixed: [(u32, u32); MAX_FIXED],
    num_fixed: u8,
    /// End of the furthest constant range: every frame must reach it.
    fixed_end: u32,
    prefix: &'p [u8],
    /// Position after the instruction's last operand.
    pub end: usize,
}

/// One element's sample: a bucket per histogram, a byte per mask, and the
/// init frame borrowed from the batch. A bucket is carried as read and judged
/// by `prepare`, so a sample read under another instruction cannot address
/// past this one's bins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CountsSample<'c> {
    buckets: [u32; MAX_COUNTS_HISTOGRAMS],
    masks: [u8; MAX_COUNTS_MASKS],
    init: &'c [u8],
}

impl<'c> CountsSample<'c> {
    /// The zero-count frame an absent row starts from.
    pub const fn init(&self) -> &'c [u8] {
        self.init
    }
}

/// Where a planned fold writes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CountsFoldTarget {
    /// The row is absent: copy the sample's init frame into the row, then apply.
    Insert,
    /// The row's stored frame, in place.
    Stored,
}

/// What [`CountsFoldOp::apply`] writes, decided before any byte moves.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CountsFoldPlan {
    target: CountsFoldTarget,
    frame_len: usize,
    count: u32,
    /// Frame-absolute offset of each histogram's element zero.
    bins_at: [u32; MAX_COUNTS_HISTOGRAMS],
    buckets: [u8; MAX_COUNTS_HISTOGRAMS],
    /// Bit `h` set: histogram `h` halves before its bucket counts.
    decay: u8,
    masks: [u8; MAX_COUNTS_MASKS],
}

impl CountsFoldPlan {
    pub const fn target(&self) -> CountsFoldTarget {
        self.target
    }

    /// The sample count after this fold.
    pub const fn count(&self) -> u32 {
        self.count
    }

    /// Whether histogram `h` halves before its bucket counts.
    pub const fn decays(&self, h: usize) -> bool {
        h < MAX_COUNTS_HISTOGRAMS && self.decay & (1 << h) != 0
    }
}

const fn overlaps(a: (u32, u32), b: (u32, u32)) -> bool {
    a.0 < b.1 && b.0 < a.1
}

/// A frame's counter ranges, ascending; the bytes outside them are metadata.
struct Counters {
    ranges: [(u32, u32); MAX_COUNTERS],
    len: usize,
    bins_at: [u32; MAX_COUNTS_HISTOGRAMS],
}

impl Counters {
    fn push(&mut self, range: (u32, u32)) {
        self.ranges[self.len] = range;
        self.len += 1;
    }

    fn all_zero(&self, frame: &[u8]) -> bool {
        self.ranges[..self.len].iter().all(|&(start, end)| {
            frame[start as usize..end as usize]
                .iter()
                .all(|&byte| byte == 0)
        })
    }

    /// Whether `a` and `b` agree outside these ranges (and in length).
    fn same_metadata(&self, a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let mut from = 0usize;
        for &(start, end) in &self.ranges[..self.len] {
            if a[from..start as usize] != b[from..start as usize] {
                return false;
            }
            from = end as usize;
        }
        a[from..] == b[from..]
    }
}

impl<'p> CountsFoldOp<'p> {
    /// Decode and validate the instruction whose first operand is `code[start]`.
    pub fn decode(code: &'p [u8], start: usize) -> Result<Self, CountsFoldError> {
        Self::decode_from(&mut Operands::at(code, start)).ok_or(CountsFoldError::InvalidProgram)
    }

    pub(crate) fn decode_from<R: OperandRead<'p>>(ops: &mut R) -> Option<Self> {
        let slot = ops.index()?;
        let key_col = ops.index()?;
        let field_idx = ops.byte()?;
        let init_offsets_col = ops.index()?;
        let init_values_col = ops.index()?;
        let count_off = ops.index()?;
        let num_hists = ops.byte()?;
        if !COUNTS_FOLD_CAPACITIES
            .histograms
            .admits(usize::from(num_hists))
        {
            return None;
        }
        let mut hists = [CountsHistogram::default(); MAX_COUNTS_HISTOGRAMS];
        for hist in &mut hists[..usize::from(num_hists)] {
            *hist = CountsHistogram {
                ref_off: ops.index()?,
                elems_at: ops.byte()?,
                bin_count: ops.byte()?,
                bucket_col: ops.index()?,
            };
            // The region's element-count word precedes element zero.
            if !COUNTS_FOLD_CAPACITIES
                .bins
                .admits(usize::from(hist.bin_count))
                || hist.elems_at < 4
            {
                return None;
            }
        }
        let num_masks = ops.byte()?;
        if !COUNTS_FOLD_CAPACITIES.masks.admits(usize::from(num_masks)) {
            return None;
        }
        let mut masks = [CountsMask::default(); MAX_COUNTS_MASKS];
        for mask in &mut masks[..usize::from(num_masks)] {
            *mask = CountsMask {
                off: ops.index()?,
                mask_col: ops.index()?,
            };
        }
        let prefix_len = ops.index()?;
        // An empty prefix would let any frame through the leading-bytes check.
        if prefix_len == 0 {
            return None;
        }
        let prefix = ops.bytes(usize::try_from(prefix_len).ok()?)?;
        let origin = prefix_len;
        // Rebase every offset onto the frame and prove the constant ranges disjoint.
        let count_off = origin.checked_add(count_off)?;
        for hist in &mut hists[..usize::from(num_hists)] {
            hist.ref_off = origin.checked_add(hist.ref_off)?;
        }
        for mask in &mut masks[..usize::from(num_masks)] {
            mask.off = origin.checked_add(mask.off)?;
        }
        let mut fixed = [(0u32, 0u32); MAX_FIXED];
        let mut len = 0usize;
        let starts = core::iter::once((count_off, COUNT_WIDTH))
            .chain(
                hists[..usize::from(num_hists)]
                    .iter()
                    .map(|hist| (hist.ref_off, REF_WIDTH)),
            )
            .chain(
                masks[..usize::from(num_masks)]
                    .iter()
                    .map(|mask| (mask.off, MASK_WIDTH)),
            );
        for (start, width) in starts {
            let range = (start, start.checked_add(width)?);
            if fixed[..len].iter().any(|&other| overlaps(range, other)) {
                return None;
            }
            fixed[len] = range;
            len += 1;
        }
        let fixed_end = fixed[..len].iter().map(|range| range.1).max()?;
        Some(Self {
            slot,
            key_col,
            field_idx,
            init_offsets_col,
            init_values_col,
            count_off,
            hists,
            num_hists,
            masks,
            num_masks,
            fixed,
            num_fixed: len as u8,
            fixed_end,
            prefix,
            end: ops.pos(),
        })
    }

    /// Compare the physical operands with the real kernel's schema-role
    /// proof, retaining the decoded operands rather than allocating a copy.
    pub fn matches_layout(&self, layout: &VoCountsLayout) -> bool {
        let origin = self.prefix.len() as u32;
        self.prefix == layout.prefix.as_slice()
            && self.count_off.checked_sub(origin) == Some(layout.count_off)
            && self.histograms().len() == layout.histograms.len()
            && self.masks().len() == layout.mask_offs.len()
            && self
                .histograms()
                .iter()
                .zip(&layout.histograms)
                .all(|(operand, field)| {
                    operand.ref_off.checked_sub(origin) == Some(field.ref_off)
                        && operand.elems_at == field.elems_at
                        && operand.bin_count == field.bin_count
                })
            && self
                .masks()
                .iter()
                .zip(&layout.mask_offs)
                .all(|(operand, &offset)| operand.off.checked_sub(origin) == Some(offset))
    }

    /// The histograms, in operand order.
    pub fn histograms(&self) -> &[CountsHistogram] {
        &self.hists[..usize::from(self.num_hists)]
    }

    /// The OR-masks, in operand order.
    pub fn masks(&self) -> &[CountsMask] {
        &self.masks[..usize::from(self.num_masks)]
    }

    /// The leading bytes every frame repeats; its length is the origin.
    pub const fn prefix(&self) -> &'p [u8] {
        self.prefix
    }

    /// Exact bytes changed by a stored-row fold. A decay owns the whole
    /// histogram; an ordinary increment owns just its selected bin.
    pub(crate) fn journal_ranges(
        &self,
        plan: &CountsFoldPlan,
        frame_offset: u32,
        ranges: &mut [(u32, u32)],
    ) -> usize {
        let mut len = 0;
        ranges[len] = (frame_offset + self.count_off, COUNT_WIDTH);
        len += 1;
        for (index, hist) in self.histograms().iter().enumerate() {
            let (offset, bytes) = if plan.decays(index) {
                (plan.bins_at[index], u32::from(hist.bin_count) * BIN_WIDTH)
            } else {
                (
                    plan.bins_at[index] + u32::from(plan.buckets[index]) * BIN_WIDTH,
                    BIN_WIDTH,
                )
            };
            ranges[len] = (frame_offset + offset, bytes);
            len += 1;
        }
        for mask in self.masks() {
            ranges[len] = (frame_offset + mask.off, MASK_WIDTH);
            len += 1;
        }
        len
    }

    pub fn read_key(&self, cols: &[&[u8]], element: u32) -> Result<u32, CountsFoldError> {
        cell_u32(cols, self.key_col, element)
    }

    /// Every batch column the instruction reads: key, init offsets and values,
    /// buckets, masks.
    pub fn for_each_column(&self, visit: &mut dyn FnMut(u32)) {
        visit(self.key_col);
        visit(self.init_offsets_col);
        visit(self.init_values_col);
        for hist in self.histograms() {
            visit(hist.bucket_col);
        }
        for mask in self.masks() {
            visit(mask.mask_col);
        }
    }

    /// Check `frame` leads with the prefix and reaches the constant ranges,
    /// then locate its counters, proving every histogram region inside the
    /// frame, of `bin_count` elements, and disjoint from the rest.
    fn counters(&self, frame: &[u8]) -> Result<Counters, CountsFoldError> {
        let mismatch = CountsFoldError::FrameMismatch;
        if frame.len() < self.fixed_end as usize || !frame.starts_with(self.prefix) {
            return Err(mismatch);
        }
        let frame_len = u32::try_from(frame.len()).map_err(|_| mismatch)?;
        let origin = self.prefix.len() as u32;
        let mut counters = Counters {
            ranges: [(0, 0); MAX_COUNTERS],
            len: 0,
            bins_at: [0; MAX_COUNTS_HISTOGRAMS],
        };
        counters.push((self.count_off, self.count_off + COUNT_WIDTH));
        for mask in self.masks() {
            counters.push((mask.off, mask.off + MASK_WIDTH));
        }
        let fixed = &self.fixed[..usize::from(self.num_fixed)];
        let mut regions = [(0u32, 0u32); MAX_COUNTS_HISTOGRAMS];
        for (h, hist) in self.histograms().iter().enumerate() {
            let region = origin
                .checked_add(bytes::read_u32(frame, hist.ref_off))
                .ok_or(mismatch)?;
            let at = region
                .checked_add(u32::from(hist.elems_at))
                .ok_or(mismatch)?;
            let end = at
                .checked_add(u32::from(hist.bin_count) * BIN_WIDTH)
                .ok_or(mismatch)?;
            let span = (region, end);
            if end > frame_len
                || bytes::read_u32(frame, region) != u32::from(hist.bin_count)
                || fixed.iter().any(|&range| overlaps(span, range))
                || regions[..h].iter().any(|&other| overlaps(span, other))
            {
                return Err(mismatch);
            }
            regions[h] = span;
            counters.bins_at[h] = at;
            counters.push((at, end));
        }
        counters.ranges[..counters.len].sort_unstable();
        Ok(counters)
    }

    /// Read element `element`'s sample: its `u32` bucket and mask cells and its
    /// init frame.
    pub fn read_sample<'c>(
        &self,
        cols: &[&'c [u8]],
        element: u32,
    ) -> Result<CountsSample<'c>, CountsFoldError> {
        let mut buckets = [0u32; MAX_COUNTS_HISTOGRAMS];
        for (bucket, hist) in buckets.iter_mut().zip(self.histograms()) {
            *bucket = cell_u32(cols, hist.bucket_col, element)?;
        }
        let mut masks = [0u8; MAX_COUNTS_MASKS];
        for (byte, mask) in masks.iter_mut().zip(self.masks()) {
            *byte = u8::try_from(cell_u32(cols, mask.mask_col, element)?)
                .map_err(|_| CountsFoldError::InvalidCellValue)?;
        }
        let start = cell_u32(cols, self.init_offsets_col, element)?;
        let end = cell_u32(
            cols,
            self.init_offsets_col,
            element
                .checked_add(1)
                .ok_or(CountsFoldError::ColumnUnderrun)?,
        )?;
        if end < start {
            return Err(CountsFoldError::InvalidCellValue);
        }
        let init = cols
            .get(self.init_values_col as usize)
            .and_then(|values| values.get(start as usize..end as usize))
            .ok_or(CountsFoldError::ColumnUnderrun)?;
        Ok(CountsSample {
            buckets,
            masks,
            init,
        })
    }

    /// Decide one fold of `sample` into the row whose stored frame is `stored`
    /// (`None` for an absent row) without writing. Every refusal happens here.
    pub fn prepare(
        &self,
        stored: Option<&[u8]>,
        sample: &CountsSample<'_>,
    ) -> Result<CountsFoldPlan, CountsFoldError> {
        let init = sample.init;
        // The init frame is element data: a frame that does not hold the layout,
        // or holds a counter that is not zero, is a value the fold cannot use.
        let init_counters = self
            .counters(init)
            .map_err(|_| CountsFoldError::InvalidCellValue)?;
        if !init_counters.all_zero(init) {
            return Err(CountsFoldError::InvalidCellValue);
        }
        let (target, frame, counters) = match stored {
            None => (CountsFoldTarget::Insert, init, init_counters),
            Some(stored) => {
                let counters = self.counters(stored)?;
                if !counters.same_metadata(stored, init) {
                    return Err(CountsFoldError::MetadataConflict);
                }
                (CountsFoldTarget::Stored, stored, counters)
            }
        };
        let count = bytes::read_u32(frame, self.count_off)
            .checked_add(1)
            .ok_or(CountsFoldError::CountOverflow)?;
        let mut buckets = [0u8; MAX_COUNTS_HISTOGRAMS];
        let mut decay = 0u8;
        for (h, (hist, &bucket)) in self.histograms().iter().zip(&sample.buckets).enumerate() {
            if bucket >= u32::from(hist.bin_count) {
                return Err(CountsFoldError::InvalidCellValue);
            }
            if bytes::read_u16(frame, counters.bins_at[h] + bucket * BIN_WIDTH) == u16::MAX {
                decay |= 1 << h;
            }
            // Below MAX_COUNTS_BINS, which fits a byte.
            buckets[h] = bucket as u8;
        }
        let mut masks = [0u8; MAX_COUNTS_MASKS];
        for ((out, mask), &bits) in masks.iter_mut().zip(self.masks()).zip(&sample.masks) {
            *out = frame[mask.off as usize] | bits;
        }
        Ok(CountsFoldPlan {
            target,
            frame_len: frame.len(),
            count,
            bins_at: counters.bins_at,
            buckets,
            decay,
            masks,
        })
    }

    /// Write `plan` into `frame`: the stored frame `prepare` read, or for
    /// [`CountsFoldTarget::Insert`] a copy of the sample's init frame. Bytes
    /// outside the count, the bins and the masks are untouched.
    pub fn apply(&self, plan: &CountsFoldPlan, frame: &mut [u8]) {
        if frame.len() != plan.frame_len {
            columine_types::die!("counts fold applied to a frame other than the one it planned");
        }
        bytes::write_u32(frame, self.count_off, plan.count);
        for (h, (hist, &bucket)) in self.histograms().iter().zip(&plan.buckets).enumerate() {
            let bins_at = plan.bins_at[h];
            if plan.decays(h) {
                for bin in 0..u32::from(hist.bin_count) {
                    let at = bins_at + bin * BIN_WIDTH;
                    bytes::write_u16(frame, at, bytes::read_u16(frame, at) >> 1);
                }
            }
            // Below u16::MAX here: either it was, or decay just halved it.
            let at = bins_at + u32::from(bucket) * BIN_WIDTH;
            let Some(next) = bytes::read_u16(frame, at).checked_add(1) else {
                columine_types::die!(
                    "counts fold applied to a frame other than the one it planned"
                );
            };
            bytes::write_u16(frame, at, next);
        }
        for (mask, &byte) in self.masks().iter().zip(&plan.masks) {
            frame[mask.off as usize] = byte;
        }
    }
}

fn cell_u32(cols: &[&[u8]], col: u32, element: u32) -> Result<u32, CountsFoldError> {
    let start = (element as usize)
        .checked_mul(4)
        .ok_or(CountsFoldError::ColumnUnderrun)?;
    cols.get(col as usize)
        .and_then(|col| col.get(start..)?.first_chunk::<4>())
        .map(|cell| u32::from_le_bytes(*cell))
        .ok_or(CountsFoldError::ColumnUnderrun)
}
