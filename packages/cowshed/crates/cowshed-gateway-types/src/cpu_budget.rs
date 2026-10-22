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
/// The control operation a resident job asks with: a relay pool and no token of its own.
pub const RESIDENT_POOL_OP: &str = "resident-pool";
/// Where a holder publishes its live CPU pool to the processes it starts, beside its count.
pub const CPU_POOL_ENV: &str = "BOUNDED_EXEC_CPU_POOL";
pub const CPU_TOKENS_ENV: &str = "BOUNDED_EXEC_CPU_TOKENS";

/// The live pool an inherited descriptor names: a nonempty endpoint without NUL beside a whole
/// count, zero included, since a resident's relay pool names zero. A pool beside no count, or
/// beside one that is no count, names nothing (05_gateway.md "Nested runners use a parent-held
/// sub-ledger").
pub fn inherited_cpu_pool(get: impl Fn(&str) -> Option<String>) -> Option<std::path::PathBuf> {
    let pool = get(CPU_POOL_ENV).filter(|pool| !pool.is_empty() && !pool.contains('\0'))?;
    get(CPU_TOKENS_ENV)
        .is_some_and(|count| count.parse::<usize>().is_ok())
        .then(|| std::path::PathBuf::from(pool))
}

/// Root jobserver admission class: the ledger's wire class (05_gateway.md "Gate admission"), and
/// the only classes that hold tokens. An exec's own class (cowshed-core `ExecClass`) maps onto it:
/// interactive asks as background, and a resident asks with [`RESIDENT_POOL_OP`] instead, so
/// neither can reach this wire. Only a live grant, not an environment marker, admits a gate.
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
    /// The job a pooled grant serves; an activation stake serves no job and has no pool.
    #[serde(skip_serializing_if = "Option::is_none")]
    job: Option<JobserverJob<'a>>,
    /// Only background jobs may borrow a requester's live pool. A gate always enters its
    /// repository's host queue; its constructor cannot attach a parent pool.
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_pool: Option<&'a str>,
}

impl<'a> JobserverRequest<'a> {
    pub const fn background(
        want: NonZeroUsize,
        checkout: &'a str,
        command: &'a str,
        job: JobserverJob<'a>,
        parent_pool: Option<&'a str>,
    ) -> Self {
        Self {
            op: JOBSERVER_OP,
            want,
            checkout,
            command,
            class: JobserverClass::Background,
            pool: true,
            job: Some(job),
            parent_pool,
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
            job: Some(job),
            parent_pool: None,
        }
    }
    /// One background token held while a shell activates, before a command exists to use it
    /// (05_gateway.md "A resident holds no token"; 11_shell.md, spare): a resident's start, or a
    /// shell pool's spare. It names no job and asks for no pool; the gateway answers `tokens: 1`
    /// and a fifo nobody reads, and the token returns when the connection closes. A start under a
    /// holder names the holder's live pool as `parent_pool`, and whichever of that pool and the host
    /// ledger grants first serves it (05_gateway.md "Work a holder starts draws on its grant").
    pub const fn activation(
        checkout: &'a str,
        command: &'a str,
        parent_pool: Option<&'a str>,
    ) -> Self {
        Self {
            op: JOBSERVER_OP,
            want: NonZeroUsize::MIN,
            checkout,
            command,
            class: JobserverClass::Background,
            pool: false,
            job: None,
            parent_pool,
        }
    }
}

/// What a resident job's supervisor writes: the checkout its descendants' asks are for, what it
/// runs, and the job identity its pool is scoped by. It names no `want`: a resident asks for no
/// token. The gateway answers `queued` and then `granted` with `cpuPool` and no `tokens`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResidentPoolRequest<'a> {
    op: &'static str,
    checkout: &'a str,
    command: &'a str,
    job: JobserverJob<'a>,
}

impl<'a> ResidentPoolRequest<'a> {
    pub const fn new(checkout: &'a str, command: &'a str, job: JobserverJob<'a>) -> Self {
        Self {
            op: RESIDENT_POOL_OP,
            checkout,
            command,
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
    /// The install that cut this ask off, beside [`INSTALL_RESTART_CODE`].
    pub install: Option<InstallRestart>,
}

/// The refusal code of a CPU ask a gateway cut off because an install is restarting it
/// (05_gateway.md "A queued CPU ask outlives an install"). It is the one refusal a client asks
/// again after: once, of the gateway the install starts, within [`InstallRestart::serves_within_ms`].
pub const INSTALL_RESTART_CODE: &str = "install-restart";

/// How long an install gives the gateway it activated to serve before it gives up on it; the bound
/// of a cut-off ask's one retry too, since the install says it in [`InstallRestart`].
pub const INSTALL_SERVES_WITHIN: std::time::Duration = std::time::Duration::from_secs(180);

/// The install restarting a gateway, as it names itself on the listener handoff socket and as the
/// gateway passes it on to every CPU ask it cuts off: the evidence a log line names.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstallRestart {
    pub pid: u32,
    pub binary: std::path::PathBuf,
    /// How long the install waits for the gateway it starts to serve ([`INSTALL_SERVES_WITHIN`]).
    pub serves_within_ms: u64,
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
    /// Its granted leases, one per background runner holding tokens, the oldest grant first.
    pub holders: Vec<CpuHolder>,
    /// Its background requests waiting for a grant, in the order it serves them.
    pub waiters: Vec<CpuWaiter>,
}

/// One waiting request, as the ledger knows it: the client that asked, what for, how many
/// tokens, and since when. Every entry is one connection: a gate job whose tasks all wait is one
/// entry per task, told apart by pid, command and age.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CpuWaiter {
    /// The client process, when its socket named one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<i32>,
    /// What the client said the tokens are for.
    pub command: String,
    /// The tokens it asked for.
    pub want: usize,
    /// Milliseconds since it asked.
    pub waited_ms: u64,
}

/// One granted lease, as the ledger knows it: the client that holds it, what it said the tokens
/// are for, and since when. A lease is one connection, granted once; its age is the grant's, not
/// its process's. The ledger never scans processes: `pid` is the socket's peer, and `ps -p <pid>`
/// tells the rest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CpuHolder {
    /// The client process, when its socket named one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<i32>,
    /// What the client said the tokens are for.
    pub command: String,
    /// The tokens it was granted.
    pub tokens: usize,
    /// The tokens it asked for.
    pub want: usize,
    /// Milliseconds since its grant.
    pub held_ms: u64,
}

/// The hold limit: a lease held this long is named first in the gateway's status, by its holder
/// and age (05_gateway.md "The hold limit"). It is reported, never enforced: a lease is a ledger
/// entry, not its work's owner, so ending it would return tokens while the work runs on over
/// budget. What stops a runaway is its own bound.
pub const HOLD_LIMIT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

impl CpuHolder {
    /// Whether this lease has been held past [`HOLD_LIMIT`].
    pub fn past_hold_limit(&self) -> bool {
        u128::from(self.held_ms) > HOLD_LIMIT.as_millis()
    }
}

/// Background's cap beside gate-class work on a host of `total` tokens: half the host, rounded
/// down, and at least one (05_gateway.md "Room for a gate"). While gate-class work holds or waits,
/// no background grant takes the class past it, so the gate takes back the rest at the next
/// returned tokens. It is also the share an exec that names no want asks for, and the count of
/// later background grants that makes a waiting background head overdue.
pub fn background_limit(total: usize) -> usize {
    (total / 2).max(1)
}

/// Whether gate-class work holds or waits for tokens on a ledger, which decides background's cap
/// ([`background_cap`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateWork {
    /// No gate-class request holds tokens or waits for them.
    Absent,
    /// A gate-class request holds tokens or waits for them.
    HoldsOrWaits,
}

/// The most tokens background-class work may be granted up to on a host of `total` tokens
/// (05_gateway.md "Room for a gate", Danny tg#9527): all of them while no gate-class work holds or
/// waits, so no token idles while background wants it, and [`background_limit`] beside gate-class
/// work. No grant shrinks, so background may hold more than the cap in force when a gate arrives;
/// it then gets no grant past the cap, and the gate takes the returned tokens first.
pub fn background_cap(total: usize, gate: GateWork) -> usize {
    match gate {
        GateWork::Absent => total,
        GateWork::HoldsOrWaits => background_limit(total),
    }
}

/// The tokens of this host's ledger: one per core the host reports (05_gateway.md "Host CPU
/// budget", "The budget").
pub fn host_tokens() -> NonZeroUsize {
    std::thread::available_parallelism().expect("every supported host reports how many CPUs it has")
}

/// The background share of this host: [`background_limit`] of [`host_tokens`], what an exec
/// that names no want asks for at background or interactive class (05_gateway.md "The task server
/// owns the live grant and nice").
pub fn background_share() -> NonZeroUsize {
    NonZeroUsize::new(background_limit(host_tokens().get())).unwrap_or(NonZeroUsize::MIN)
}

impl CpuBudgetStatus {
    /// This ledger's [`background_limit`]: background's cap beside gate-class work, and the
    /// share an exec that names no want asks for.
    pub fn background_limit(&self) -> usize {
        background_limit(self.total)
    }

    /// Whether gate-class work holds or waits on this ledger.
    pub fn gate_work(&self) -> GateWork {
        if self.gate_jobs.is_empty() && self.gate_queue.is_empty() {
            GateWork::Absent
        } else {
            GateWork::HoldsOrWaits
        }
    }

    /// The cap in force on this ledger's background grants ([`background_cap`]).
    pub fn background_cap(&self) -> usize {
        background_cap(self.total, self.gate_work())
    }

    /// The tokens background-class work holds, over the cap in force when a gate arrived after
    /// background's grants.
    pub fn background_held(&self) -> usize {
        self.background.iter().map(|stake| stake.held).sum()
    }
}

/// One waiting gate-class request: the gate job it serves, and the request itself.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CpuGateWaiter {
    pub checkout: String,
    /// The gate job it serves, as that job's admission number: the smaller is the older job.
    pub job: u64,
    /// The client process, when its socket named one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<i32>,
    /// What the client said the tokens are for.
    pub command: String,
    /// The tokens it asked for.
    pub want: usize,
    /// Milliseconds since it asked.
    pub waited_ms: u64,
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
