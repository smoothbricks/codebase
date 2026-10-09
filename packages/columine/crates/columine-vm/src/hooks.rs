//! Service boundary between container operations and VM-owned state.
//!
//! HashMap, HashSet and BITMAP operations use this trait for undo logging and
//! TTL eviction-index maintenance. The VM supplies the implementation;
//! [`NoVm`] is the deliberately minimal environment used when those services
//! are not wired. BITMAP work itself is not a hook: the VM routes a set
//! opcode on a BITMAP-typed slot to `bitmap_ops` with its own `BitmapEnv`.

use crate::meta::SlotMetaView;
use columine_types::types::ErrorCode;

/// Physical ranges for successive container erases. A hash-table, array or
/// condition-tree erase names a fixed number of ranges, held inline; only a
/// bitmap removal's payload ranges vary with the patch.
#[derive(Debug, Default)]
pub struct EraseScratch {
    pub ranges: crate::hash_table::EraseRanges,
    pub payload_ranges: Vec<(u32, u32)>,
}

/// TTL physical authority for a combined family mutation. Only the live
/// prefix and possible new destinations are captured, never index capacity.
pub fn ttl_capture_ranges(
    state: &[u8],
    meta: &SlotMetaView,
    additional_entries: u32,
) -> Result<[(u32, u32); 2], ErrorCode> {
    use columine_types::types::SlotMetaOffset;
    if !meta.has_ttl() {
        return Ok([(0, 0); 2]);
    }
    let metadata_end = meta
        .meta_base
        .checked_add(SlotMetaOffset::EVICTION_INDEX_SIZE + 4)
        .ok_or(ErrorCode::InvalidState)?;
    if metadata_end as usize > state.len() {
        return Err(ErrorCode::InvalidState);
    }
    let size = meta.eviction_index_size(state);
    let capacity = meta.eviction_index_capacity(state);
    if size > capacity {
        return Err(ErrorCode::InvalidState);
    }
    let entries = size
        .checked_add(additional_entries)
        .ok_or(ErrorCode::InvalidState)?
        .min(capacity);
    let len = entries
        .checked_mul(crate::state_init::EVICTION_ENTRY_SIZE)
        .ok_or(ErrorCode::InvalidState)?;
    let base = meta.eviction_index_offset(state);
    let end = base.checked_add(len).ok_or(ErrorCode::InvalidState)?;
    if end as usize > state.len() {
        return Err(ErrorCode::InvalidState);
    }
    Ok([
        (meta.meta_base + SlotMetaOffset::EVICTION_INDEX_SIZE, 4),
        (base, len),
    ])
}

/// Exclusive authority over pre-reserved physical before-bytes. Tokens are
/// consumed in LIFO order; a disabled token means only that undo is disabled.
#[derive(Debug)]
#[must_use]
pub struct ByteCapture {
    pub(crate) frame: Option<CaptureFrame>,
}

#[derive(Debug)]
pub(crate) struct CaptureFrame {
    pub(crate) generation: u64,
    pub(crate) image: usize,
    pub(crate) depth: u32,
    pub(crate) scratch_start: usize,
    pub(crate) ranges_start: usize,
    pub(crate) entries: u32,
}

/// Inputs to one retained, atomically admitted TTL batch plan.
#[derive(Clone, Copy, Debug)]
pub enum TtlMutation {
    Upsert {
        key: u32,
        timestamp: f64,
        mapped_value: u32,
    },
    Remove {
        key: u32,
    },
}

/// Services container operations may request from the VM.
pub trait VmHooks {
    /// Whether undo logging is enabled.
    fn undo_enabled(&self) -> bool;

    fn take_erase_scratch(&mut self) -> EraseScratch;
    fn restore_erase_scratch(&mut self, scratch: EraseScratch);

    /// Validate and reserve every possible physical record before mutation.
    fn begin_capture(
        &mut self,
        state: &[u8],
        ranges: &[(u32, u32)],
    ) -> Result<ByteCapture, ErrorCode>;

    /// Consume the matching authority and journal the actual changed bytes.
    fn finish_capture(
        &mut self,
        delta_mode: bool,
        state: &[u8],
        ranges: &[(u32, u32)],
        capture: ByteCapture,
    );

    /// Retain one or many TTL mutations under the same physical authority.
    /// Single-key families supply `iter::once`; no second planner exists.
    fn plan_ttl_batch(
        &mut self,
        state: &[u8],
        meta: &SlotMetaView,
        mutations: impl IntoIterator<Item = TtlMutation>,
    ) -> Result<crate::ttl_plan::TtlBatchPlan, ErrorCode>;

    fn consume_ttl_batch_plan(
        &mut self,
        state: &mut [u8],
        meta: &SlotMetaView,
        plan: crate::ttl_plan::TtlBatchPlan,
    );
}

/// Minimal environment with undo and TTL services disabled. Calling an
/// unimplemented service is a programmer error, so it panics rather than
/// silently doing nothing.
#[derive(Debug, Default)]
pub struct NoVm {
    erase_scratch: EraseScratch,
}

impl VmHooks for NoVm {
    fn undo_enabled(&self) -> bool {
        false
    }

    fn take_erase_scratch(&mut self) -> EraseScratch {
        core::mem::take(&mut self.erase_scratch)
    }

    fn restore_erase_scratch(&mut self, scratch: EraseScratch) {
        self.erase_scratch = scratch;
    }

    fn begin_capture(
        &mut self,
        state: &[u8],
        ranges: &[(u32, u32)],
    ) -> Result<ByteCapture, ErrorCode> {
        for &(offset, len) in ranges {
            let end = offset.checked_add(len).ok_or(ErrorCode::InvalidState)?;
            if end as usize > state.len() {
                return Err(ErrorCode::InvalidState);
            }
        }
        Ok(ByteCapture { frame: None })
    }

    fn finish_capture(
        &mut self,
        _delta_mode: bool,
        _state: &[u8],
        _ranges: &[(u32, u32)],
        capture: ByteCapture,
    ) {
        assert!(capture.frame.is_none(), "enabled capture passed to NoVm");
    }

    fn plan_ttl_batch(
        &mut self,
        _state: &[u8],
        _meta: &SlotMetaView,
        _mutations: impl IntoIterator<Item = TtlMutation>,
    ) -> Result<crate::ttl_plan::TtlBatchPlan, ErrorCode> {
        unreachable!("TTL plan requires the VM eviction environment")
    }

    fn consume_ttl_batch_plan(
        &mut self,
        _state: &mut [u8],
        _meta: &SlotMetaView,
        _plan: crate::ttl_plan::TtlBatchPlan,
    ) {
        unreachable!("TTL plan requires the VM eviction environment")
    }
}
