//! The host CPU budget's wire shape, shared by the gateway that holds it and every process that
//! starts a parallel runner under it (specs/cowshed/05_gateway.md, "Host CPU budget").
//!
//! Every gate on the host sized its test runners to the whole machine: an Nx gate runs one task
//! per core, and each nextest runner under it runs one test thread per core again, so five
//! concurrent gates kept the load average at 80–170 on 18 cores and tests that do no disk work
//! timed out waiting for a CPU. The gateway therefore holds one budget of CPU tokens for the host,
//! about one per core, and a runner takes tokens before it starts and sizes its own parallelism to
//! what it was granted.
//!
//! A client connects to the gateway's control socket, writes one [`CpuTokensRequest`] line and
//! keeps the connection open without half-closing it. The gateway answers a
//! [`LeaseState::Queued`] line at once and a [`LeaseState::Granted`] line carrying `tokens` when
//! the runner may start, or one refusal (`ok: false` with a code and an error). The tokens are
//! held until the client closes the connection, so a runner killed while holding them returns them
//! with its socket, and one that closes while queued leaves the queue.
//!
//! [`LeaseState`]: crate::LeaseState

use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

/// The control operation that asks for CPU tokens.
pub const CPU_TOKENS_OP: &str = "cpu-tokens";

/// The one line a client writes:
/// `{"op":"cpu-tokens","want":8,"checkout":"/path/to/checkout","command":"…"}`.
///
/// `want` is the most tokens the runner can use: the threads or processes it would run at once
/// unbudgeted. The grant is between one and `want`, never more than the host total. `checkout`
/// names the workspace the runner works for; grants are shared fairly between checkouts, not
/// between connections, so a gate with many runners cannot crowd out one with few. `command`
/// names what the tokens are for, as the gateway reports its holders.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct CpuTokensRequest<'a> {
    op: &'static str,
    pub want: NonZeroUsize,
    pub checkout: &'a str,
    pub command: &'a str,
}

impl<'a> CpuTokensRequest<'a> {
    pub const fn new(want: NonZeroUsize, checkout: &'a str, command: &'a str) -> Self {
        Self {
            op: CPU_TOKENS_OP,
            want,
            checkout,
            command,
        }
    }
}

/// The budget as the gateway reports it in its status: the host total, how many tokens are held,
/// and by whom. `held` plus the free tokens is always `total`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CpuBudgetStatus {
    pub total: usize,
    pub held: usize,
    /// One entry per checkout holding or waiting for tokens.
    pub checkouts: Vec<CpuCheckoutStatus>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CpuCheckoutStatus {
    pub checkout: String,
    /// Tokens its granted runners hold.
    pub held: usize,
    /// Runners holding tokens.
    pub running: usize,
    /// Runners waiting for a grant.
    pub waiting: usize,
}
