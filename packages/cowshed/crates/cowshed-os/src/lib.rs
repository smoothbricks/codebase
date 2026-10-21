//! Host primitives shared by every cowshed layer: child processes and their reports, durable file
//! I/O, the fork lock that keeps descriptors out of children, lifecycle and latency spans, the
//! host's run-queue load, and diagnostic lines on the stderr every cowshed process shares.

pub mod fork_lock;
pub mod fsio;
pub mod host_load;
pub mod process;
pub mod scratch;
pub mod stderr;
pub mod test_backstop;
pub mod timing;
