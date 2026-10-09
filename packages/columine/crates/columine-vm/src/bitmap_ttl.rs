//! Timestamp inputs for bitmap algebra, not a payload or TTL-index writer.
//!
//! Bitmap algebra preserves exact optional timestamp bits. An index pass
//! retains its key/order evidence, sorted sides skip sorting, and one reusable
//! scatter orders only unsorted sides.

use crate::bitmap_ops::BitmapAlgebraOp;
use crate::bytes;
use crate::hooks::{TtlMutation, ttl_capture_ranges};
use crate::meta::SlotMetaView;
use crate::state_init::EVICTION_ENTRY_SIZE;
use bitmosaic::BitmosaicView;
use columine_types::types::ErrorCode;
use core::cmp::Ordering;

#[derive(Clone, Copy, Debug)]
struct ExpiryEntry {
    key: u32,
    bits: u64,
}

/// Reused join inputs and output mutations. No encoded index image is copied.
#[derive(Debug, Default)]
pub(crate) struct TtlAlgebraScratch {
    target: Vec<ExpiryEntry>,
    source: Vec<ExpiryEntry>,
    scatter: Vec<ExpiryEntry>,
    removals: Vec<u32>,
    upserts: Vec<ExpiryEntry>,
    /// Capacities when a batch's reservation was sealed: no merge of the batch
    /// may grow any buffer past them.
    #[cfg(debug_assertions)]
    sealed: Option<[usize; 5]>,
}

/// The largest TTL algebra one batch or call can run: the most live index
/// rows of a target and of a source.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TtlAlgebraDemand {
    /// Live index rows of the target slot.
    pub(crate) target_rows: usize,
    /// Live index rows of the source slot.
    pub(crate) source_rows: usize,
}

impl TtlAlgebraDemand {
    /// Raise each field to the larger of the two, so a batch's demand covers
    /// every merge widened into it.
    pub(crate) fn widen(&mut self, other: Self) {
        self.target_rows = self.target_rows.max(other.target_rows);
        self.source_rows = self.source_rows.max(other.source_rows);
    }
}

/// Grow `buf` so its capacity is at least `total` elements, keeping its
/// contents. Refuses with `OutOfMemory` instead of aborting.
fn reserve_to<T>(buf: &mut Vec<T>, total: usize) -> Result<(), ErrorCode> {
    buf.try_reserve(total.saturating_sub(buf.len()))
        .map_err(|_| ErrorCode::OutOfMemory)
}

impl TtlAlgebraScratch {
    /// Removals precede upserts so the existing batch planner can admit the
    /// whole result against its actual remaining index capacity.
    pub(crate) fn mutations(&self) -> impl Iterator<Item = TtlMutation> + '_ {
        self.removals
            .iter()
            .copied()
            .map(|key| TtlMutation::Remove { key })
            .chain(self.upserts.iter().map(|entry| TtlMutation::Upsert {
                key: entry.key,
                timestamp: f64::from_bits(entry.bits),
                mapped_value: entry.key,
            }))
    }

    fn clear(&mut self) {
        self.target.clear();
        self.source.clear();
        self.removals.clear();
        self.upserts.clear();
    }

    /// Back the largest algebra a batch can run, before the batch's first
    /// write, so every merge of the batch finds the storage it needs. Capacity
    /// only grows; buffer contents are kept.
    pub(crate) fn reserve_for(&mut self, demand: TtlAlgebraDemand) -> Result<(), ErrorCode> {
        let upsert_rows = demand
            .target_rows
            .checked_add(demand.source_rows)
            .ok_or(ErrorCode::OutOfMemory)?;
        reserve_to(&mut self.target, demand.target_rows)?;
        reserve_to(&mut self.source, demand.source_rows)?;
        reserve_to(
            &mut self.scatter,
            demand.target_rows.max(demand.source_rows),
        )?;
        reserve_to(&mut self.removals, demand.target_rows)?;
        reserve_to(&mut self.upserts, upsert_rows)
    }

    /// From here until [`Self::unseal`], no merge may grow this storage.
    pub(crate) fn seal(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.sealed = Some(self.capacities());
        }
    }

    /// Lift the seal taken by [`Self::seal`] once the batch is done.
    pub(crate) fn unseal(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.sealed = None;
        }
    }

    /// Current capacity of each buffer a seal covers, in the order of `sealed`.
    #[cfg(debug_assertions)]
    fn capacities(&self) -> [usize; 5] {
        [
            self.target.capacity(),
            self.source.capacity(),
            self.scatter.capacity(),
            self.removals.capacity(),
            self.upserts.capacity(),
        ]
    }

    /// Debug check: a merge under a seal left every buffer's capacity as it was.
    fn check_sealed(&self) {
        #[cfg(debug_assertions)]
        {
            if let Some(sealed) = self.sealed {
                debug_assert_eq!(
                    self.capacities(),
                    sealed,
                    "TTL algebra grew past its batch's reservation"
                );
            }
        }
    }
}

/// Read each live index row once; stable ordering later preserves which
/// duplicate was last in the original retained index.
fn read_entries(
    state: &[u8],
    meta: &SlotMetaView,
    entries: &mut Vec<ExpiryEntry>,
) -> Result<bool, ErrorCode> {
    if !meta.has_ttl() {
        return Ok(true);
    }
    let [_, (offset, len)] = ttl_capture_ranges(state, meta, 0)?;
    let start = usize::try_from(offset).map_err(|_| ErrorCode::InvalidState)?;
    let len = usize::try_from(len).map_err(|_| ErrorCode::InvalidState)?;
    let end = start.checked_add(len).ok_or(ErrorCode::InvalidState)?;
    let rows = state.get(start..end).ok_or(ErrorCode::InvalidState)?;
    let width = usize::try_from(EVICTION_ENTRY_SIZE).map_err(|_| ErrorCode::InvalidState)?;
    reserve_to(entries, rows.len() / width)?;
    let mut previous = None;
    let mut sorted = true;
    for row in rows.chunks_exact(width) {
        let key = bytes::read_u32(row, 8);
        let bits = bytes::read_u64(row, 0);
        if previous.is_some_and(|last| last > key) {
            sorted = false;
        }
        entries.push(ExpiryEntry { key, bits });
        previous = Some(key);
    }
    Ok(sorted)
}

/// Stable four-pass LSD radix order. The scatter keeps its high-water length
/// across calls; only the active prefix is read, so warming a smaller side
/// does not erase or reinitialize the larger side's retained workspace.
/// Refuses with `OutOfMemory` if the scatter cannot grow to the entry count.
fn radix_order(
    entries: &mut [ExpiryEntry],
    scatter: &mut Vec<ExpiryEntry>,
) -> Result<(), ErrorCode> {
    let len = entries.len();
    if scatter.len() < len {
        reserve_to(scatter, len)?;
        scatter.resize(len, ExpiryEntry { key: 0, bits: 0 });
    }
    let mut buckets = [[0usize; 256]; 4];
    for entry in entries.iter() {
        for (pass, byte) in entry.key.to_le_bytes().into_iter().enumerate() {
            buckets[pass][usize::from(byte)] += 1;
        }
    }
    for (pass, positions) in buckets.iter_mut().enumerate() {
        let mut start = 0;
        for position in positions.iter_mut() {
            let count = *position;
            *position = start;
            start += count;
        }
        let (input, output) = if pass % 2 == 0 {
            (&*entries, &mut scatter[..len])
        } else {
            (&scatter[..len], &mut *entries)
        };
        for entry in input {
            let bucket = usize::from(entry.key.to_le_bytes()[pass]);
            output[positions[bucket]] = *entry;
            positions[bucket] += 1;
        }
    }
    Ok(())
}

fn group_expiry(entries: &[ExpiryEntry], at: &mut usize, key: u32) -> Option<u64> {
    let mut last = None;
    while let Some(entry) = entries.get(*at)
        && entry.key == key
    {
        last = Some(entry.bits);
        *at += 1;
    }
    last
}

fn output_member(op: BitmapAlgebraOp, target: bool, source: bool) -> bool {
    match op {
        BitmapAlgebraOp::And => target && source,
        BitmapAlgebraOp::Or => target || source,
        BitmapAlgebraOp::AndNot => target && !source,
        BitmapAlgebraOp::Xor => target != source,
    }
}

fn ordered_expiry(op: BitmapAlgebraOp, target: Option<u64>, source: Option<u64>) -> Option<u64> {
    match (target, source) {
        (None, expiry) | (expiry, None) => expiry,
        (Some(a), Some(b)) => {
            let comparison = f64::from_bits(a).partial_cmp(&f64::from_bits(b));
            match (op, comparison) {
                (BitmapAlgebraOp::Or, Some(Ordering::Less))
                | (BitmapAlgebraOp::And, Some(Ordering::Greater)) => Some(b),
                // Equal and unordered comparisons preserve A's exact bits.
                _ => Some(a),
            }
        }
    }
}

fn output_expiry(
    op: BitmapAlgebraOp,
    target_member: bool,
    target: Option<u64>,
    source: Option<u64>,
) -> Option<u64> {
    match op {
        BitmapAlgebraOp::And | BitmapAlgebraOp::Or => ordered_expiry(op, target, source),
        BitmapAlgebraOp::AndNot => target,
        BitmapAlgebraOp::Xor => {
            if target_member {
                target
            } else {
                source
            }
        }
    }
}

/// Join the admitted views and indexes into existing TTL mutation inputs.
/// This function writes no state and never opens another bitmap image.
/// A source with TTL cannot be admitted into a target without TTL; the public
/// algebra admission checks that rule, and this check protects raw callers.
#[allow(clippy::too_many_arguments)]
pub(crate) fn merge_expiries(
    state: &[u8],
    target_meta: &SlotMetaView,
    source_meta: Option<&SlotMetaView>,
    target: Option<BitmosaicView<'_>>,
    source: Option<BitmosaicView<'_>>,
    op: BitmapAlgebraOp,
    scratch: &mut TtlAlgebraScratch,
) -> Result<bool, ErrorCode> {
    scratch.clear();
    if !target_meta.has_ttl() {
        if source_meta.is_some_and(SlotMetaView::has_ttl) {
            return Err(ErrorCode::InvalidProgram);
        }
        return Ok(false);
    }
    let same_slot = source_meta.is_some_and(|meta| meta.meta_base == target_meta.meta_base);
    if same_slot && matches!(op, BitmapAlgebraOp::And | BitmapAlgebraOp::Or) {
        return Ok(false);
    }
    if !read_entries(state, target_meta, &mut scratch.target)? {
        radix_order(&mut scratch.target, &mut scratch.scatter)?;
    }
    if !same_slot
        && let Some(meta) = source_meta
        && !read_entries(state, meta, &mut scratch.source)?
    {
        radix_order(&mut scratch.source, &mut scratch.scatter)?;
    }
    reserve_to(&mut scratch.removals, scratch.target.len())?;
    let upper = scratch
        .target
        .len()
        .checked_add(scratch.source.len())
        .ok_or(ErrorCode::InvalidState)?;
    reserve_to(&mut scratch.upserts, upper)?;
    let mut target_at = 0;
    let mut source_at = 0;
    loop {
        let key = match (scratch.target.get(target_at), scratch.source.get(source_at)) {
            (Some(a), Some(b)) => a.key.min(b.key),
            (Some(entry), None) | (None, Some(entry)) => entry.key,
            (None, None) => break,
        };
        let target_raw = group_expiry(&scratch.target, &mut target_at, key);
        let source_raw = if same_slot {
            target_raw
        } else {
            group_expiry(&scratch.source, &mut source_at, key)
        };
        let in_target = target.is_some_and(|view| view.contains(key));
        let in_source = if same_slot {
            in_target
        } else {
            source.is_some_and(|view| view.contains(key))
        };
        let target_expiry = if in_target { target_raw } else { None };
        let source_expiry = if in_source { source_raw } else { None };
        if output_member(op, in_target, in_source) {
            match output_expiry(op, in_target, target_expiry, source_expiry) {
                Some(bits) if !in_target || target_expiry != Some(bits) => {
                    scratch.upserts.push(ExpiryEntry { key, bits });
                }
                // A newly added, untracked member cannot inherit a stale
                // target entry. Other already-stale target rows are untouched.
                None if !in_target && target_raw.is_some() => scratch.removals.push(key),
                _ => {}
            }
        } else if in_target && target_raw.is_some() {
            scratch.removals.push(key);
        }
    }
    scratch.check_sealed();
    Ok(!scratch.removals.is_empty() || !scratch.upserts.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_expiries_preserve_ties_and_unordered_bits() {
        let negative_zero = (-0.0f64).to_bits();
        let positive_zero = 0.0f64.to_bits();
        let nan_a = 0x7ff8_0000_0000_00a1;
        let nan_b = 0x7ff8_0000_0000_00b2;
        for op in [BitmapAlgebraOp::And, BitmapAlgebraOp::Or] {
            assert_eq!(ordered_expiry(op, None, None), None);
            assert_eq!(ordered_expiry(op, Some(nan_a), None), Some(nan_a));
            assert_eq!(ordered_expiry(op, None, Some(nan_b)), Some(nan_b));
            assert_eq!(
                ordered_expiry(op, Some(negative_zero), Some(positive_zero)),
                Some(negative_zero)
            );
            assert_eq!(ordered_expiry(op, Some(nan_a), Some(nan_b)), Some(nan_a));
            assert_eq!(
                ordered_expiry(op, Some(1.0f64.to_bits()), Some(nan_b)),
                Some(1.0f64.to_bits())
            );
        }
        assert_eq!(
            ordered_expiry(
                BitmapAlgebraOp::Or,
                Some(1.0f64.to_bits()),
                Some(2.0f64.to_bits())
            ),
            Some(2.0f64.to_bits())
        );
        assert_eq!(
            ordered_expiry(
                BitmapAlgebraOp::And,
                Some(1.0f64.to_bits()),
                Some(2.0f64.to_bits())
            ),
            Some(1.0f64.to_bits())
        );
        assert_eq!(
            output_expiry(BitmapAlgebraOp::AndNot, true, Some(nan_a), Some(nan_b)),
            Some(nan_a)
        );
        assert_eq!(
            output_expiry(BitmapAlgebraOp::Xor, true, Some(nan_a), None),
            Some(nan_a)
        );
        assert_eq!(
            output_expiry(BitmapAlgebraOp::Xor, false, None, Some(nan_b)),
            Some(nan_b)
        );
    }

    #[test]
    fn radix_order_is_stable_and_keeps_last_original_duplicate() -> Result<(), ErrorCode> {
        let mut entries = vec![
            ExpiryEntry {
                key: u32::MAX,
                bits: 1,
            },
            ExpiryEntry { key: 7, bits: 2 },
            ExpiryEntry {
                key: 0x0100_0000,
                bits: 3,
            },
            ExpiryEntry { key: 7, bits: 4 },
            ExpiryEntry { key: 0, bits: 5 },
        ];
        let mut scatter = Vec::new();
        radix_order(&mut entries, &mut scatter)?;
        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.key, entry.bits))
                .collect::<Vec<_>>(),
            [(0, 5), (7, 2), (7, 4), (0x0100_0000, 3), (u32::MAX, 1)]
        );
        let mut at = 1;
        assert_eq!(group_expiry(&entries, &mut at, 7), Some(4));
        assert_eq!(at, 3);
        let high_water = scatter.len();
        let mut smaller = [
            ExpiryEntry { key: 2, bits: 6 },
            ExpiryEntry { key: 1, bits: 7 },
        ];
        radix_order(&mut smaller, &mut scatter)?;
        assert_eq!((smaller[0].key, smaller[1].key), (1, 2));
        assert_eq!(scatter.len(), high_water);
        Ok(())
    }
}
