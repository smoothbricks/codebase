//! Host primitives shared by every cowshed layer: child processes and their reports, durable file
//! I/O, the fork lock that keeps descriptors out of children, lifecycle and latency spans, bounded
//! waits on another process, the host's run-queue load, diagnostic lines on the stderr every cowshed
//! process shares, and stdout and stderr writes that wait out a description another process left
//! nonblocking.

pub mod deadline;
pub mod fork_lock;
pub mod fsio;
pub mod host_load;
pub mod namespace;
pub mod process;
pub mod scratch;
pub mod stderr;
pub mod stdio;
pub mod test_backstop;
pub mod timing;
