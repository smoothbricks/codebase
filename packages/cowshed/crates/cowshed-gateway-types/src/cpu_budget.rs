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
pub const JOBSERVER_OP: &str = "jobserver";

/// Root jobserver admission class. Only a live grant, not an environment marker, admits a gate.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum JobserverClass {
    #[default]
    Background,
    Gate,
}

/// The trusted supervisor's existing immutable job identity; never an environment claim.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobserverJob<'a> {
    pub repo_id: &'a str,
    pub workspace_incarnation: &'a str,
    pub job_id: std::num::NonZeroU64,
}

/// A supervisor holds one root grant; its children draw only from the private live pool.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobserverRequest<'a> {
    op: &'static str,
    want: NonZeroUsize,
    checkout: &'a str,
    command: &'a str,
    class: JobserverClass,
    pool: bool,
    job: JobserverJob<'a>,
}

impl<'a> JobserverRequest<'a> {
    pub const fn background(
        want: NonZeroUsize,
        checkout: &'a str,
        command: &'a str,
        job: JobserverJob<'a>,
    ) -> Self {
        Self {
            op: JOBSERVER_OP,
            want,
            checkout,
            command,
            class: JobserverClass::Background,
            pool: true,
            job,
        }
    }
    pub const fn gate(
        want: NonZeroUsize,
        checkout: &'a str,
        command: &'a str,
        job: JobserverJob<'a>,
    ) -> Self {
        Self {
            op: JOBSERVER_OP,
            want,
            checkout,
            command,
            class: JobserverClass::Gate,
            pool: true,
            job,
        }
    }
}

/// The grant or refusal as a supervisor reads it; unrelated control fields are ignored.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobserverAnswer {
    pub ok: bool,
    pub lease: Option<crate::LeaseState>,
    pub tokens: Option<NonZeroUsize>,
    pub fifo: Option<std::path::PathBuf>,
    pub cpu_pool: Option<std::path::PathBuf>,
    pub code: Option<String>,
    pub error: Option<String>,
}

/// The gateway and the single sandbox builder derive exactly the same workspace IPC scope.
/// The global control socket and every other workspace remain outside that scope.
pub fn cpu_pool_directory(
    store: &std::path::Path,
    workspace_incarnation: &str,
) -> Result<std::path::PathBuf, crate::ConfigError> {
    if workspace_incarnation.len() != 32
        || !workspace_incarnation
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(crate::ConfigError::InvalidCpuPoolIncarnation);
    }
    use base64::Engine as _;
    let digit = |byte: u8| {
        if byte <= b'9' {
            byte - b'0'
        } else {
            byte - b'a' + 10
        }
    };
    let mut identity = [0_u8; 16];
    for (slot, pair) in identity
        .iter_mut()
        .zip(workspace_incarnation.as_bytes().as_chunks::<2>().0)
    {
        *slot = digit(pair[0]) * 16 + digit(pair[1]);
    }
    let mut encoded = [0_u8; 22];
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode_slice(identity, &mut encoded)
        .expect("22 bytes hold a complete base64url 128-bit incarnation");
    let name = std::str::from_utf8(&encoded).expect("base64url is ASCII");
    Ok(store.join("run/cpu").join(name))
}

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
    /// One entry per checkout holding or waiting for tokens, every class together.
    pub checkouts: Vec<CpuCheckoutStatus>,
    /// The gate jobs whose runners hold tokens, oldest job first: the first is the head of the
    /// gate queue, the land every returned token goes to while it wants one.
    pub gate_jobs: Vec<CpuGateJob>,
    /// The gate-class requests waiting, in the order returned tokens go to them: the oldest gate
    /// job's first, and within one job in the order its tasks asked.
    pub gate_queue: Vec<CpuGateWaiter>,
    /// Each checkout's background-class stake alone, for the checkouts that have one: what is
    /// served behind the gate jobs.
    pub background: Vec<CpuBackgroundStake>,
}

/// One gate job's runners that hold tokens, in one checkout.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CpuGateJob {
    pub checkout: String,
    /// The gate job, as its admission number: the smaller is the older job.
    pub job: u64,
    /// Tokens its granted runners hold: the sum of its holders' tokens.
    pub held: usize,
    /// Its granted leases, one per runner holding tokens, the oldest grant first.
    pub holders: Vec<CpuHolder>,
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

/// One checkout's background-class stake: what it holds, who of it waits, and each granted lease.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CpuBackgroundStake {
    pub checkout: String,
    /// Tokens its granted background runners hold: the sum of its holders' tokens.
    pub held: usize,
    /// Background runners waiting for a grant.
    pub waiting: usize,
    /// Its granted leases, one per background runner holding tokens, the oldest grant first.
    pub holders: Vec<CpuHolder>,
}

/// One granted lease, as the ledger knows it: the client that holds it, what it said the tokens
/// are for, and since when. A lease is one connection, granted once; its age is the grant's, not
/// its process's, and a growth does not renew it. The ledger never scans processes: `pid` is the
/// socket's peer, and `ps -p <pid>` tells the rest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CpuHolder {
    /// The client process, when its socket named one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<i32>,
    /// What the client said the tokens are for.
    pub command: String,
    /// The tokens it holds now, growth included.
    pub tokens: usize,
    /// The tokens it asked for.
    pub want: usize,
    /// Milliseconds since its grant.
    pub held_ms: u64,
}

/// One waiting gate-class request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CpuGateWaiter {
    pub checkout: String,
    /// The gate job it serves, as that job's admission number: the smaller is the older job.
    pub job: u64,
    /// The tokens it asked for.
    pub want: usize,
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use std::path::Path;

    #[test]
    fn pool_scope_encoding_preserves_every_incarnation_bit() {
        let mut scopes = std::collections::BTreeSet::new();
        for bit in 0..=128 {
            let mut identity = [0_u8; 16];
            if bit < 128 {
                identity[bit / 8] = 1 << (bit % 8);
            }
            let incarnation: String = identity.iter().map(|byte| format!("{byte:02x}")).collect();
            let directory = super::cpu_pool_directory(Path::new("/store"), &incarnation).unwrap();
            assert_eq!(directory.parent(), Some(Path::new("/store/run/cpu")));
            let encoded = directory.file_name().unwrap().to_str().unwrap();
            assert_eq!(encoded.len(), 22);
            assert_eq!(
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(encoded)
                    .unwrap(),
                identity
            );
            assert!(
                scopes.insert(directory),
                "distinct full identities must have distinct scopes"
            );
        }
        assert_eq!(scopes.len(), 129);
    }

    #[test]
    fn pool_scope_is_store_local_and_fits_the_existing_fixture_socket_budget() {
        let incarnation = "0123456789abcdef0123456789abcdef";
        let store = Path::new("/private/tmp/cowshed-dtest-79675-0-cli/store");
        let scope = super::cpu_pool_directory(store, incarnation).unwrap();
        let endpoint = scope.join("abcdefghijklmnopqrstuv");
        assert!(scope.starts_with(store));
        assert_eq!(endpoint.as_os_str().as_encoded_bytes().len(), 98);
        assert!(endpoint.as_os_str().as_encoded_bytes().len() < 104);
        assert_ne!(
            scope,
            super::cpu_pool_directory(Path::new("/another-store"), incarnation).unwrap()
        );
        for invalid in [
            "",
            "../0123456789abcdef0123456789abcd",
            "0123456789ABCDEF0123456789abcdef",
            "0123456789abcdef0123456789abcdeg",
        ] {
            assert!(super::cpu_pool_directory(store, invalid).is_err());
        }
    }
}
