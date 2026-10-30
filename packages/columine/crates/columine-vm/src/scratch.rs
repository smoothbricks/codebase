//! Working memory one batch or call can need, derived before its first
//! write. Every buffer the VM grows while it mutates state is reserved from
//! this demand first, so `OutOfMemory` refuses a whole batch with nothing
//! written and never stops one partway.

use crate::bitmap_ops::BitmapPatchDemand;
use crate::nested::NestedPlanDemand;
use crate::ttl_plan::TtlPlanDemand;

/// Per scratch family, the largest use one batch or call can make.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ScratchDemand {
    /// The TTL planner's largest plan.
    pub(crate) ttl: TtlPlanDemand,
    /// The largest inner-table rehash of a nested slot; `None` when the
    /// batch makes no nested plan at all.
    pub(crate) nested: Option<NestedPlanDemand>,
    /// The largest bitmap patch; `None` when the batch patches no bitmap.
    pub(crate) bitmap: Option<BitmapPatchDemand>,
    /// Keys the largest stored-row sweep can select.
    pub(crate) sweep_keys: usize,
    /// Cuts the batch's removals can record (`removal_cut`): one per
    /// execution of a removal opcode.
    pub(crate) removal_cuts: usize,
}
