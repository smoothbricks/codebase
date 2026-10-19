//! Host primitives shared by every cowshed layer: child processes and their reports, durable file
//! I/O, the fork lock that keeps descriptors out of children, lifecycle and latency spans, and the
//! host's run-queue load.

pub mod fork_lock;
pub mod fsio;
pub mod host_load;
pub mod process;
#[cfg(any(test, feature = "test-hooks"))]
pub mod test_backstop;
pub mod timing;
