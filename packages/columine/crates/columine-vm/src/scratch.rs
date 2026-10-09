//! Working memory one batch or call can need, derived before its first
//! write. Every buffer the VM grows while it mutates state is reserved from
//! this demand first, so `OutOfMemory` refuses a whole batch with nothing
//! written and never stops one partway.

use crate::ttl_plan::TtlPlanDemand;

/// Per scratch family, the largest use one batch or call can make.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ScratchDemand {
    /// The TTL planner's largest plan.
    pub(crate) ttl: TtlPlanDemand,
}
