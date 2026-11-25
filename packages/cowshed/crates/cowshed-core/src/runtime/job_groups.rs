//! A served supervisor's job groups and observed process lives, kept beside its socket so
//! whoever finds the supervisor gone can retire them (11_shell.md "Supervisor recovery").
//!
//! The ledger is a complete snapshot, replaced whole (write a temporary, rename) when a job
//! starts, its observed membership changes, or it seals. Each group carries its leader's
//! birth time as the job's parent observed it before reaping ([`Birth`]), never renewed from
//! the pid. Observed lives carry their kernel identity and boot session separately.
//!
//! Recovery is not the parent and cannot prevent pid reuse. It therefore never signals a
//! recorded group with killpg. Group discovery requires the recorded leader to hold the group
//! id both before and after the read; each discovered member is signalled through its kernel
//! pid version ([`Member`]). Once that leader is reaped, the group id alone proves no ownership:
//! an unresolved group is retained and reported until its id is empty.
//!
//! Independently retained, boot-fenced process lives remain owned after setsid, reparenting,
//! exec, or leader release. Recovery signals only a matching unique life, refreshing its pid
//! version across exec without renewing its identity. Their exit proof is required before
//! those claims leave the same ledger; a reused pid or a different boot grants no authority.
//!
//! What names a process, and reads, signals and watches it, is the platform's (`macos.rs`: audit
//! tokens, `proc_pidinfo` unique ids and `KERN_PROC_PID`; `linux.rs`: pidfds and procfs),
//! selected here and nowhere else. Callers see one interface.

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use cowshed_os::process::ExitWatch;

use super::process_tree::ProcessIdentity;
use crate::api::dto::ExitStatus;

#[cfg(target_os = "linux")]
pub(super) mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

pub use platform::{Process, start_time};
use platform::{birth_time, end_recorded_members, members_of, recorded_life};
pub(crate) use platform::{life_of, process_record, signal_life};

/// How often a group signalled with SIGKILL is read again for processes still running in it,
/// and how many times before the group is reported as surviving SIGKILL.
const KILL_SETTLE: Duration = Duration::from_millis(10);
const KILL_ROUNDS: u32 = 100;

/// The ledger beside a supervisor socket.
pub fn ledger_path(socket: &Path) -> PathBuf {
    socket.with_extension("groups")
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Ledger {
    /// The supervisor process that wrote it.
    supervisor: u32,
    groups: Vec<Group>,
    /// Its groups were ended after the supervisor was lost; the jobs still await sealing.
    #[serde(default)]
    ended: bool,
    /// Groups a lost supervisor recorded that no later one could end or forget; see
    /// [`UnresolvedGroup`]. Every ledger of the workspace carries them until nothing holds their
    /// ids.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    unresolved: Vec<UnresolvedGroup>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Group {
    job_id: u64,
    pgid: i32,
    /// The leader's start time as its parent observed it ([`start_time`]): the identity the
    /// kernel answers for every process, whoever owns it, so a pid another user's process now
    /// holds is told from the leader rather than refused unread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    leader_start: Option<u64>,
    /// What earlier builds recorded instead: the leader's `proc_pid_rusage` birth
    /// ([`birth_time`]), a per-boot clock the kernel reads back only for the caller's own
    /// processes. Only read, so their ledgers stay recoverable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    leader_birth: Option<u64>,
    /// Boot-fenced observed lives remain ours after setsid, reparenting and leader release.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    members: Option<RecordedMembers>,
}

/// Observed lives are told apart within one boot, never across a reboot: macOS's unique ids and
/// Linux's start ticks both count from it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RecordedMembers {
    boot_session: String,
    lives: Vec<ProcessIdentity>,
}

/// One job's group and observed lives, written by the supervisor that already owns both. The
/// lives are the job's published process observation, shared with it rather than copied.
#[derive(Clone, Debug)]
pub struct JobGroupClaim {
    pub job_id: u64,
    pub leader: GroupLeader,
    /// `None` when nothing observes lives outside the group's proof.
    pub members: Option<Arc<[ProcessIdentity]>>,
}

impl JobGroupClaim {
    /// A claim on the group alone, with no observed lives.
    pub fn group(job_id: u64, leader: GroupLeader) -> Self {
        Self {
            job_id,
            leader,
            members: None,
        }
    }
}

/// A recorded group whose vanished leader no longer proves ownership of its current members.
/// The id could now name a stranger's group, so it is never signalled as a whole. Independently
/// retained exact lives can still be retired under their own boot-fenced authority. Every later
/// ledger carries the unresolved group, and doctor reports it, until its id and owned lives are
/// empty.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnresolvedGroup {
    /// The supervisor process that recorded the group and was lost.
    lost_supervisor: u32,
    group: Group,
}

impl UnresolvedGroup {
    pub fn job_id(&self) -> u64 {
        self.group.job_id
    }

    pub fn pgid(&self) -> i32 {
        self.group.pgid
    }

    pub fn lost_supervisor(&self) -> u32 {
        self.lost_supervisor
    }

    /// Why the group could not be ended, derived from what its record holds.
    pub fn reason(&self) -> UnresolvedReason {
        if self.group.leader_birth.is_some() || self.group.leader_start.is_some() {
            UnresolvedReason::LeaderGone
        } else {
            UnresolvedReason::LeaderNeverIdentified
        }
    }

    /// Whether processes still hold the id. Anything else -- the id held by nobody, or led by a
    /// process that is not the recorded leader -- means the job's group has ended.
    fn still_held(&self) -> io::Result<bool> {
        Ok(!matches!(ownership(&self.group)?, Ownership::NotOwned)
            || recorded_members_alive(&self.group)?)
    }
}

/// Why a group a lost supervisor recorded could not be ended ([`UnresolvedGroup`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnresolvedReason {
    /// Its recorded leader was reaped: the processes holding the id are no longer provably the
    /// job's.
    LeaderGone,
    /// An earlier build recorded the group without its leader's identity.
    LeaderNeverIdentified,
}

/// A job's process-group leader as its parent observed it: the group id, the leader's birth time
/// (its rusage clock, which [`crate::runtime::job_accounting`] matches the leader's usage reads
/// against) and its start time (the identity a ledger records). Carried unchanged into every
/// ledger rewrite; see [`Birth`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupLeader {
    pgid: i32,
    birth: u64,
    start: u64,
}

impl GroupLeader {
    /// Observe the leader of the group `pid` leads, which must be the caller's own child, not yet
    /// reaped. A process already reaped has no identity left to observe, so that is an error.
    pub fn observe(pid: u32) -> io::Result<Self> {
        let pgid = i32::try_from(pid)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let leader = Self::from_parts(pgid, 0, 0)?;
        let reaped = || {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("process {pgid} was already reaped"),
            )
        };
        let birth = birth_time(pgid)?.ok_or_else(reaped)?;
        let start = start_time(pgid)?.ok_or_else(reaped)?;
        Ok(Self {
            birth,
            start,
            ..leader
        })
    }

    /// A leader another process observed and reported over the shell-host wire.
    pub(super) fn from_parts(pgid: i32, birth: u64, start: u64) -> io::Result<Self> {
        if pgid <= 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a job process group must be positive",
            ));
        }
        Ok(Self { pgid, birth, start })
    }

    pub fn pgid(self) -> i32 {
        self.pgid
    }

    pub fn birth(self) -> u64 {
        self.birth
    }

    pub fn start(self) -> u64 {
        self.start
    }
}

/// A job's process as its parent saw it before anything could reap it: the identity of the group
/// it leads, or why that identity could not be read.
///
/// Only the parent observes it truthfully. Once the leader is reaped its pid may name any later
/// process, so an observation made afterwards -- by the supervisor when an event reaches it, or
/// by a ledger rewrite -- could record a stranger as a group the ledger may end. The parent
/// observes before it waits, the supervisor carries the result, and nothing observes it again.
/// A leader that already exited but is not yet reaped still has its birth time ([`birth_time`]),
/// so a parent's observation names it even then.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Birth {
    Observed(GroupLeader),
    /// The leader's identity could not be read. The job still runs and reports its pid, but its
    /// group never enters a ledger: nothing may end a group no one could identify.
    Unobserved {
        pid: u32,
        reason: String,
    },
}

impl Birth {
    /// Observe `pid`, which leads a group and is the caller's own child, not yet reaped.
    pub fn of(pid: u32) -> Self {
        match GroupLeader::observe(pid) {
            Ok(leader) => Self::Observed(leader),
            Err(error) => Self::Unobserved {
                pid,
                reason: error.to_string(),
            },
        }
    }

    pub fn pid(&self) -> u32 {
        match self {
            Self::Observed(leader) => leader.pgid.unsigned_abs(),
            Self::Unobserved { pid, .. } => *pid,
        }
    }

    pub fn leader(&self) -> Option<GroupLeader> {
        match self {
            Self::Observed(leader) => Some(*leader),
            Self::Unobserved { .. } => None,
        }
    }
}

/// What a ledger rewrite carried over from the unresolved groups the supervisor inherited.
#[derive(Debug)]
pub struct Recorded {
    /// The inherited groups processes still hold, and those that could not be inspected: an
    /// inherited group is dropped only once it is seen released.
    pub carried: Vec<UnresolvedGroup>,
    /// Why some inherited groups could not be inspected. They are carried unchanged.
    pub uninspected: Vec<io::Error>,
}

/// The two paths of the supervisor's atomic group publication, prepared once when it starts,
/// and the boot session its observed lives are fenced by, read once when the first is
/// published. No group state is retained here: every publication borrows the actor's current
/// claims.
pub struct Recorder {
    path: PathBuf,
    temporary: PathBuf,
    boot_session: OnceLock<String>,
}

impl Recorder {
    pub fn new(path: PathBuf) -> Self {
        let temporary = path.with_extension(format!("groups.{}", std::process::id()));
        Self {
            path,
            temporary,
            boot_session: OnceLock::new(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Replace the ledger with the current claims and inherited unresolved groups not seen
    /// released. An inspection failure keeps its group and is reported to the supervisor.
    pub fn record<I>(&self, groups: I, inherited: &[UnresolvedGroup]) -> io::Result<Recorded>
    where
        I: Iterator<Item = JobGroupClaim> + Clone,
    {
        let mut carried = Vec::with_capacity(inherited.len());
        let mut uninspected = Vec::new();
        for group in inherited {
            match group.still_held() {
                Ok(false) => {}
                Ok(true) => carried.push(group.clone()),
                Err(error) => {
                    carried.push(group.clone());
                    uninspected.push(error);
                }
            }
        }
        let ledger = Publication {
            supervisor: std::process::id(),
            groups: Groups {
                claims: groups,
                boot_session: &self.boot_session,
            },
            ended: false,
            unresolved: &carried,
        };
        publish(&self.path, &self.temporary, &ledger)?;
        Ok(Recorded {
            carried,
            uninspected,
        })
    }
}

/// The write view preserves Ledger's field order and omission rules without owning its arrays.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Publication<'a, G> {
    supervisor: u32,
    groups: G,
    ended: bool,
    #[serde(skip_serializing_if = "<[UnresolvedGroup]>::is_empty")]
    unresolved: &'a [UnresolvedGroup],
}

struct Groups<'a, I> {
    claims: I,
    boot_session: &'a OnceLock<String>,
}

impl<I> Serialize for Groups<'_, I>
where
    I: Iterator<Item = JobGroupClaim> + Clone,
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error as _, SerializeSeq as _};
        let mut groups = serializer.serialize_seq(None)?;
        for claim in self.claims.clone() {
            let members = match claim.members.as_deref() {
                Some(lives) if !lives.is_empty() => Some(MembersView {
                    boot_session: boot_session(self.boot_session).map_err(S::Error::custom)?,
                    lives,
                }),
                _ => None,
            };
            groups.serialize_element(&GroupView {
                job_id: claim.job_id,
                pgid: claim.leader.pgid,
                leader_start: claim.leader.start,
                members,
            })?;
        }
        groups.end()
    }
}

/// A [`Group`] as a publication writes it: the leader's start, never an earlier build's birth.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GroupView<'a> {
    job_id: u64,
    pgid: i32,
    leader_start: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    members: Option<MembersView<'a>>,
}

/// [`RecordedMembers`] borrowed from the claim and the recorder.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MembersView<'a> {
    boot_session: &'a str,
    lives: &'a [ProcessIdentity],
}

/// The boot session, read from the kernel once per recorder: it cannot change while this
/// process lives.
fn boot_session(cell: &OnceLock<String>) -> io::Result<&str> {
    if let Some(session) = cell.get() {
        return Ok(session);
    }
    let session = platform::boot_session()?;
    Ok(cell.get_or_init(|| session))
}

fn still_held(groups: &[UnresolvedGroup]) -> io::Result<Vec<UnresolvedGroup>> {
    let mut held = Vec::with_capacity(groups.len());
    for group in groups {
        if group.still_held()? {
            held.push(group.clone());
        }
    }
    Ok(held)
}

fn replace(path: &Path, ledger: &Ledger) -> io::Result<()> {
    let temporary = path.with_extension(format!("groups.{}", std::process::id()));
    publish(path, &temporary, ledger)
}

fn publish(path: &Path, temporary: &Path, ledger: &impl Serialize) -> io::Result<()> {
    let bytes = serde_json::to_vec(ledger).map_err(io::Error::other)?;
    std::fs::write(temporary, bytes)?;
    std::fs::rename(temporary, path)
}

/// Whose ledger may be acted on.
#[derive(Clone, Copy, Debug)]
pub enum Writer {
    /// Only the one this supervisor process wrote: a watcher of that process, which must not
    /// touch a ledger a newer supervisor of the workspace has written since.
    Process(u32),
    /// Whichever supervisor wrote it: the caller holds the workspace's socket, so every
    /// earlier supervisor is gone.
    Any,
}

impl Writer {
    fn wrote(self, ledger: &Ledger) -> bool {
        match self {
            Self::Process(pid) => pid == ledger.supervisor,
            Self::Any => true,
        }
    }
}

/// Whether the ledger at `path` that `writer` wrote names a group [`end_recorded`] would end.
/// A supervisor that retired in order left none.
pub fn names_groups(path: &Path, writer: Writer) -> io::Result<bool> {
    Ok(read(path)?.is_some_and(|ledger| writer.wrote(&ledger) && !ledger.groups.is_empty()))
}

/// What ending a ledger's groups did.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct Ended {
    /// Jobs whose groups were signalled.
    pub signalled: Vec<u64>,
    /// Jobs whose groups processes still hold although their recorded leader is gone: not
    /// signalled ([`UnresolvedGroup`]).
    pub unresolved: Vec<u64>,
}

/// End every group the ledger at `path` names that is still the job's — TERM, `grace`, then
/// KILL — when `writer` wrote it, and mark it ended; the ledger stays until the next supervisor
/// of the workspace takes it ([`take_lost`]).
pub fn end_recorded(path: &Path, writer: Writer, grace: Duration) -> io::Result<Ended> {
    let Some(mut ledger) = read(path)? else {
        return Ok(Ended::default());
    };
    if !writer.wrote(&ledger) {
        return Ok(Ended::default());
    }
    let (signalled, unresolved) = end_groups(&ledger.groups, grace)?;
    ledger.ended = true;
    replace(path, &ledger)?;
    Ok(Ended {
        signalled,
        unresolved: unresolved.iter().map(|group| group.job_id).collect(),
    })
}

/// For the supervisor now holding the workspace's socket: end whatever its lost predecessor's
/// ledger still names that is still the job's, and take over the ledger. Which jobs to seal as
/// lost is the job records' to say, not the ledger's: power loss can take the ledger with the
/// processes it names.
///
/// Returns the unresolved groups -- the predecessor's, and those it inherited -- that processes
/// still hold. They are written into a ledger of this process, which its supervisor carries on
/// ([`record`]); with none, the ledger is removed.
pub fn take_lost(path: &Path, grace: Duration) -> io::Result<Vec<UnresolvedGroup>> {
    let Some(ledger) = read(path)? else {
        return Ok(Vec::new());
    };
    // A prior signaling pass is not authority to discard a still-live group's evidence.
    let (_, newly) = end_groups(&ledger.groups, grace)?;
    let inherited: Vec<_> = ledger
        .unresolved
        .iter()
        .map(|claim| claim.group.clone())
        .collect();
    end_groups(&inherited, grace)?;
    let mut unresolved = still_held(&ledger.unresolved)?;
    unresolved.extend(newly.into_iter().map(|group| UnresolvedGroup {
        lost_supervisor: ledger.supervisor,
        group,
    }));
    if unresolved.is_empty() {
        std::fs::remove_file(path)?;
    } else {
        replace(
            path,
            &Ledger {
                supervisor: std::process::id(),
                groups: Vec::new(),
                ended: false,
                unresolved: unresolved.clone(),
            },
        )?;
    }
    Ok(unresolved)
}

/// The first recorded job that may still run, without signalling or rewriting its ledger.
pub fn live_job(path: &Path) -> io::Result<Option<u64>> {
    let Some(ledger) = read(path)? else {
        return Ok(None);
    };
    for group in ledger
        .groups
        .iter()
        .chain(ledger.unresolved.iter().map(|held| &held.group))
    {
        let held = match ownership(group)? {
            Ownership::Owned(members) => !members.is_empty(),
            Ownership::Unidentified => true,
            Ownership::NotOwned => false,
        };
        if held || recorded_members_alive(group)? {
            return Ok(Some(group.job_id));
        }
    }
    Ok(None)
}
/// The unresolved groups the ledger at `path` carries that processes still hold, observed now.
/// Reads only: the ledger's writer drops released ones.
pub fn unresolved(path: &Path) -> io::Result<Vec<UnresolvedGroup>> {
    match read(path)? {
        Some(ledger) => still_held(&ledger.unresolved),
        None => Ok(Vec::new()),
    }
}

fn read(path: &Path) -> io::Result<Option<Ledger>> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Whether a recorded group may be signalled as the job's.
enum Ownership {
    /// The recorded leader held the group id while these processes were read holding it: they
    /// were the job's.
    Owned(Vec<Process>),
    /// Nothing holds the group id, or its leader is another process.
    NotOwned,
    /// The recorded leader is gone (or was never identified) and processes hold the group id.
    /// They may be the job's, or a stranger's that took the id after the job's group had ended:
    /// no signal.
    Unidentified,
}

/// Retire the provable groups, then the boot-fenced lives retained independently of each group.
/// Any refusal leaves the original ledger available; a lost leader never revokes a known life.
fn end_groups(groups: &[Group], grace: Duration) -> io::Result<(Vec<u64>, Vec<Group>)> {
    let mut running = Vec::with_capacity(groups.len());
    let mut unresolved = Vec::new();
    for group in groups {
        match ownership(group)? {
            Ownership::Owned(members) if members.is_empty() => {}
            Ownership::Owned(members) => running.push((group.clone(), members)),
            Ownership::NotOwned => {}
            Ownership::Unidentified => unresolved.push(group.clone()),
        }
    }
    let groups_ended = end_running(&running, grace);
    let members_ended = end_recorded_members(groups, grace);
    let (mut signalled, lost) = match (groups_ended, members_ended) {
        (Ok((mut signalled, lost)), Ok(members)) => {
            for job in members {
                if !signalled.contains(&job) {
                    signalled.push(job);
                }
            }
            (signalled, lost)
        }
        (Err(groups), Err(members)) => {
            return Err(io::Error::new(
                groups.kind(),
                format!("group retirement: {groups}; recorded member retirement: {members}"),
            ));
        }
        (Err(error), _) | (_, Err(error)) => return Err(error),
    };
    unresolved.extend(lost);
    let mut retained = Vec::new();
    for group in unresolved {
        if !matches!(ownership(&group)?, Ownership::NotOwned) || recorded_members_alive(&group)? {
            retained.push(group);
        }
    }
    signalled.sort_unstable();
    Ok((signalled, retained))
}

/// Whether a recorded life still runs. A different boot proves the recorded lives ended, without
/// consulting recycled pids.
fn recorded_members_alive(group: &Group) -> io::Result<bool> {
    let Some(members) = &group.members else {
        return Ok(false);
    };
    if members.boot_session != platform::boot_session()? {
        return Ok(false);
    }
    for life in &members.lives {
        if recorded_life(*life)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// TERM each group's processes as they were read, wait `grace` once for them all, then KILL
/// whatever of each group still runs as the job's. A TERM that reached none of a group's
/// processes does not spare it the KILL: on macOS a process that execs takes a new pid version,
/// so one read just before its exec is not reached under the identity read, while it still runs
/// as the job's. Returns the jobs a TERM or a KILL reached and the groups whose leader went
/// while processes held on.
fn end_running(
    running: &[(Group, Vec<Process>)],
    grace: Duration,
) -> io::Result<(Vec<u64>, Vec<Group>)> {
    let mut termed = Vec::with_capacity(running.len());
    for (_, members) in running {
        let mut reached = false;
        for member in members {
            reached |= member.signal(libc::SIGTERM)?;
        }
        termed.push(reached);
    }
    let mut signalled = Vec::with_capacity(running.len());
    let mut unresolved = Vec::new();
    if running.is_empty() {
        return Ok((signalled, unresolved));
    }
    std::thread::sleep(grace);
    for ((group, _), termed) in running.iter().zip(termed) {
        let killed = kill_owned(group)?;
        if termed || killed.reached {
            signalled.push(group.job_id);
        }
        if killed.end == KillEnd::Unidentified {
            // The leader died and was reaped while processes held on.
            unresolved.push(group.clone());
        }
    }
    Ok((signalled, unresolved))
}

/// What KILLing a group's processes until none runs did.
#[derive(Debug)]
struct Killed {
    /// A KILL reached at least one of the job's processes.
    reached: bool,
    end: KillEnd,
}

/// How KILLing a group's processes until none runs ended.
#[derive(Debug, Eq, PartialEq)]
enum KillEnd {
    /// Nothing of the job's runs in the group any more.
    Ended,
    /// The recorded leader is gone while processes hold the id: they are not signalled.
    Unidentified,
}

fn kill_owned(group: &Group) -> io::Result<Killed> {
    let mut reached = false;
    for _ in 0..KILL_ROUNDS {
        let end = match ownership(group)? {
            Ownership::Owned(members) if members.is_empty() => KillEnd::Ended,
            Ownership::Owned(members) => {
                for member in &members {
                    reached |= member.signal(libc::SIGKILL)?;
                }
                std::thread::sleep(KILL_SETTLE);
                continue;
            }
            Ownership::NotOwned => KillEnd::Ended,
            Ownership::Unidentified => KillEnd::Unidentified,
        };
        return Ok(Killed { reached, end });
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "process group {} still has running processes after {KILL_ROUNDS} rounds of SIGKILL",
            group.pgid
        ),
    ))
}

fn ownership(group: &Group) -> io::Result<Ownership> {
    if group.pgid <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a recorded job process group must be positive",
        ));
    }
    let unverifiable = |recorded: &str, error: io::Error| {
        io::Error::new(
            error.kind(),
            format!(
                "cannot verify process group {} against its recorded {recorded}: {error}",
                group.pgid
            ),
        )
    };
    // `Some(true)` while the recorded leader holds its pid, `Some(false)` while another process
    // does; `None` once no process holds it, or the group never had an identified leader. A
    // start time is read for any process, so a pid another user's process took is `Some(false)`.
    // An earlier build's birth is read back only for this user's processes: where another
    // user's process holds the pid the kernel declines, and nothing tells it from the leader.
    let leader = || -> io::Result<Option<bool>> {
        Ok(match (group.leader_start, group.leader_birth) {
            (Some(recorded), _) => start_time(group.pgid)
                .map_err(|error| unverifiable("start time", error))?
                .map(|now| now == recorded),
            (None, Some(recorded)) => birth_time(group.pgid)
                .map_err(|error| unverifiable("rusage birth from an earlier build", error))?
                .map(|now| now == recorded),
            (None, None) => None,
        })
    };
    match leader()? {
        Some(false) => return Ok(Ownership::NotOwned),
        Some(true) => {
            let members = members_of(group.pgid)?;
            // The recorded leader held its pid -- the group's id -- before the processes were
            // read and still holds it after, so it held it throughout: no other group can have
            // had the id meanwhile, and every process read holding it was the job's.
            if leader()? == Some(true) {
                return Ok(Ownership::Owned(members));
            }
        }
        None => {}
    }
    Ok(if members_of(group.pgid)?.is_empty() {
        Ownership::NotOwned
    } else {
        Ownership::Unidentified
    })
}

/// Whether a process that has not exited holds the group id.
pub fn group_has_live_members(pgid: i32) -> io::Result<bool> {
    Ok(!members_of(pgid)?.is_empty())
}

/// Every running process in the job's group, which `leader` leads, each held by the identity it
/// had when read ([`Process`]).
///
/// While its parent holds the leader unreaped, the leader holding its pid -- the group id --
/// before the members are read and still after proves every process read the job's; an exited
/// leader is no member but still proves the group, which may then be empty. Once the leader is
/// reaped nothing proves the id the job's, but an id no running process holds names an empty
/// group whoever held it. Anything else -- an unidentified leader, a pid that names another
/// process, processes holding the id of a leader gone, a membership not read in full -- is an
/// error, never a guessed or empty membership.
pub fn job_members(leader: &Birth) -> io::Result<Vec<Process>> {
    proven_members(leader, members_of)
}

/// [`job_members`]'s membership as the kernel records each member: its pid, its parent and the
/// tokens of both lives ([`ProcessRecord`]), read under the same held-leader proof.
pub(crate) fn job_member_records(leader: &Birth) -> io::Result<Vec<ProcessRecord>> {
    proven_members(leader, platform::member_records)
}

/// The processes `members` reads holding the group id, proven the job's as [`job_members`]
/// describes.
fn proven_members<T>(
    leader: &Birth,
    members: impl Fn(i32) -> io::Result<Vec<T>>,
) -> io::Result<Vec<T>> {
    let pgid = i32::try_from(leader.pid()).map_err(io::Error::other)?;
    // `Some(true)` while the leader holds its pid, `Some(false)` while another process does,
    // `None` once no process holds it. Its start time answers whoever the leader runs as, alive or
    // exited and held unreaped; its rusage birth is declined (`EPERM`) once it execs a setuid-root
    // program.
    let holds = |leader: GroupLeader| -> io::Result<Option<bool>> {
        Ok(start_time(leader.pgid)?.map(|start| start == leader.start))
    };
    if let Birth::Observed(leader) = leader {
        match holds(*leader)? {
            Some(true) => {
                let members = members(pgid)?;
                if holds(*leader)? == Some(true) {
                    return Ok(members);
                }
            }
            Some(false) => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "process {pgid} no longer leads the job's group: its pid names another process"
                    ),
                ));
            }
            None => {}
        }
    }
    if members(pgid)?.is_empty() {
        return Ok(Vec::new());
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        match leader {
            Birth::Observed(_) => format!(
                "the job's group leader {pgid} is gone while processes hold its group id: \
                 nothing proves them the job's"
            ),
            Birth::Unobserved { reason, .. } => format!(
                "the job's group leader {pgid} was never identified ({reason}): nothing proves \
                 the processes holding its group id the job's"
            ),
        },
    ))
}

/// The local ports of the TCP sockets in `LISTEN` that the running processes of the job's group,
/// which `leader` leads, hold now: IPv4 and IPv6 alike, each port once. The membership is
/// [`job_members`]'s, and each member's sockets count only while that very process is shown
/// still running after they were read ([`Process::listening_ports`]), so a pid reused meanwhile
/// lends the job no listener. A member that exited holds none. A membership or socket table that
/// cannot be read is an error, never an empty list.
pub fn job_listening_ports(leader: &Birth) -> io::Result<BTreeSet<u16>> {
    let mut ports = BTreeSet::new();
    for member in job_members(leader)? {
        if let Some(held) = member.listening_ports()? {
            ports.extend(held);
        }
    }
    Ok(ports)
}

/// Signal the group `pgid` leads. Only for the leader's parent, while it holds the leader
/// unreaped -- running, or exited and not yet collected: until the parent reaps it, the leader's
/// pid, and with it the group's id, names nothing else. A group with nothing left running in it
/// is not an error: Darwin refuses one whose only member is the unreaped leader.
pub fn signal_unreaped_group(pgid: i32, signal: libc::c_int) -> io::Result<()> {
    // SAFETY: killpg takes plain integers and touches no memory of ours.
    if unsafe { libc::killpg(pgid, signal) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(()),
        Some(libc::EPERM) if matches!(group_has_live_members(pgid), Ok(false)) => Ok(()),
        _ => Err(io::Error::new(
            error.kind(),
            format!("cannot signal process group {pgid} with signal {signal}: {error}"),
        )),
    }
}

/// How long a held group whose controller is gone has between SIGTERM and SIGKILL: the grace a
/// supervisor gives its own jobs (11_shell.md "Process identity and signal ownership").
pub const LOST_CONTROLLER_GRACE: Duration = Duration::from_secs(2);

/// How often a group ended for a lost controller is looked at for running processes within the
/// grace.
const LOST_CONTROLLER_POLL: Duration = Duration::from_millis(10);

/// What ending a held group did, step by step: every step is attempted whatever an earlier one
/// answered, and each keeps its own native result.
#[derive(Debug)]
pub struct GroupEnding {
    pub term: io::Result<()>,
    pub membership: io::Result<()>,
    pub kill: io::Result<()>,
}

impl GroupEnding {
    /// Every step succeeded.
    pub fn is_clean(&self) -> bool {
        self.term.is_ok() && self.membership.is_ok() && self.kill.is_ok()
    }
}

impl std::fmt::Display for GroupEnding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "TERM={:?}; membership={:?}; KILL={:?}",
            self.term, self.membership, self.kill
        )
    }
}

/// End the group the caller's own unreaped child `pgid` leads, as a group whose controller is
/// gone is ended (11_shell.md): SIGTERM through `signal`, the grace while any process of the group
/// still runs, then SIGKILL. `signal` is the caller's own fenced send, so no signal leaves its
/// fence; the caller reaps the leader afterwards. Blocks the calling thread for at most the grace.
pub fn end_held_group(
    pgid: i32,
    mut signal: impl FnMut(libc::c_int) -> io::Result<()>,
) -> GroupEnding {
    let term = signal(libc::SIGTERM);
    let deadline = std::time::Instant::now() + LOST_CONTROLLER_GRACE;
    let membership = loop {
        match group_has_live_members(pgid) {
            Ok(true) if std::time::Instant::now() < deadline => {
                std::thread::sleep(LOST_CONTROLLER_POLL);
            }
            Ok(_) => break Ok(()),
            Err(cause) => break Err(cause),
        }
    };
    let kill = signal(libc::SIGKILL);
    GroupEnding {
        term,
        membership,
        kill,
    }
}

/// How the caller's own child `pid` ended, read without reaping it; `None` while it runs. The
/// child stays unreaped, so its pid -- and the id of the group it leads -- names nothing else
/// until its parent collects it.
pub fn exit_unreaped(pid: i32) -> io::Result<Option<ExitStatus>> {
    read_exit(pid)
}

/// Wait for the caller's own child `pid` to end and say how, without reaping it, within the reap
/// bound (specs/cowshed/05_gateway.md, "Deadlines"). For a caller told the child is exiting: its
/// exit is watched (`deadline::await_exit`), and Darwin reports a process exiting (`NOTE_EXIT`,
/// or `ESRCH` when the exit is watched) before `waitid` can collect it (measured: 17 of 2000
/// children were not yet waitable right after either), so the exit is then read without blocking
/// each timer tick until it answers (`deadline::collect_exited`). A child still running or still in
/// its exit at the bound fails `TimedOut`, naming its pid and kernel state, and stays unreaped.
pub fn await_exit_unreaped(pid: i32) -> io::Result<ExitStatus> {
    use cowshed_os::deadline;
    const ROLE: &str = "child told to exit";
    let id =
        u32::try_from(pid).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let budget = deadline::start("read child exit", deadline::REAP);
    deadline::await_exit(ROLE, id, &budget, deadline::Progress::Nothing)
        .and_then(|()| {
            deadline::collect_exited(ROLE, id, &budget, deadline::Progress::Nothing, || {
                read_exit(pid)
            })
        })
        .map_err(io::Error::from)
}

// WHY unbounded: `WNOHANG` returns at once.
#[allow(clippy::disallowed_methods)]
fn read_exit(pid: i32) -> io::Result<Option<ExitStatus>> {
    let id = libc::id_t::try_from(pid)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    loop {
        // SAFETY: an all-zero `siginfo_t` is valid storage for the call to fill.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: `pid` is the caller's own child and `info` writable storage; WNOWAIT leaves the
        // child unreaped.
        let waited = unsafe {
            libc::waitid(
                libc::P_PID,
                id,
                &mut info,
                libc::WEXITED | libc::WNOWAIT | libc::WNOHANG,
            )
        };
        if waited != 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        // SAFETY: waitid filled `info` as a child-state record, or left it zeroed.
        let (reporter, status) = unsafe { (info.si_pid(), info.si_status()) };
        if reporter == 0 {
            return Ok(None);
        }
        match info.si_code {
            libc::CLD_EXITED => return Ok(Some(ExitStatus::Exited { code: status })),
            libc::CLD_KILLED => {
                return Ok(Some(ExitStatus::Signaled {
                    signal: status,
                    core_dumped: false,
                }));
            }
            libc::CLD_DUMPED => {
                return Ok(Some(ExitStatus::Signaled {
                    signal: status,
                    core_dumped: true,
                }));
            }
            // Darwin reports a stop (and a continue) to a wait that asked only for exits
            // (measured on Darwin 25.6), and WNOWAIT leaves the report pending, so every later
            // read would meet it again. A stop is not an exit: the report is taken and the exit
            // read again.
            libc::CLD_STOPPED | libc::CLD_CONTINUED => take_stop_report(id)?,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("process {pid} reported child state {other}, not an exit"),
                ));
            }
        }
    }
}

/// Take child `id`'s pending stop or continue report without reaping it: the wait asks for
/// those states alone, never `WEXITED`, so an exited child stays unreaped for its fence.
// WHY unbounded: `WNOHANG` returns at once.
#[allow(clippy::disallowed_methods)]
fn take_stop_report(id: libc::id_t) -> io::Result<()> {
    loop {
        // SAFETY: an all-zero `siginfo_t` is valid storage for the call to fill.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: `id` is the caller's own child and `info` writable storage; without WEXITED
        // the wait cannot collect an exit.
        let waited = unsafe {
            libc::waitid(
                libc::P_PID,
                id,
                &mut info,
                libc::WSTOPPED | libc::WCONTINUED | libc::WNOHANG,
            )
        };
        if waited == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();

        // Linux has no eligible stop/continue state once this child is a zombie, so it
        // reports ECHILD even though WEXITED can still observe our unreaped exit. Only
        // that positive, non-reaping observation distinguishes it from a non-child.
        if error.raw_os_error() == Some(libc::ECHILD) {
            let pid = i32::try_from(id)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            if exit_unreaped(pid)?.is_some() {
                return Ok(());
            }
        }
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// A running process as the kernel names it: its pid, its parent's pid, the kernel's token for
/// the life of each, its effective uid and the calendar second it started.
///
/// The tokens are the platform's. macOS: `p_uniqueid`, which the kernel never gives another
/// process while the system runs. Linux: the `starttime` of `/proc/<pid>/stat`, in clock ticks
/// since boot, which tells the lives of one pid apart and names nothing without that pid: two
/// pids may share it, and a pid given away within one tick of its last life's start repeats it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessRecord {
    pub pid: u32,
    pub ppid: u32,
    pub unique_id: u64,
    pub parent_unique_id: u64,
    pub uid: u32,
    pub started_seconds: u64,
}

impl ProcessRecord {
    /// The life this record is of ([`Life`]).
    pub(crate) fn life(&self) -> Life {
        Life {
            pid: self.pid.cast_signed(),
            unique_id: self.unique_id,
        }
    }
}

/// One process through every image it runs: its pid and the kernel's token for that life
/// ([`ProcessRecord::unique_id`]). An exec keeps the token, so a life is found again after one,
/// while a [`Process`] held from before it may not be (macOS gives the pid a new version). On
/// Linux the token is no identity without the pid, so the two are only ever compared together.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Life {
    pub(crate) pid: i32,
    pub(crate) unique_id: u64,
}

/// A process the trusted ledger proves is a job's own: a member of a recorded group whose
/// ownership held throughout ([`ownership`]). Only [`owned_process`] makes one, so a caller holding
/// it has the proof and no other process can enter the privileged signal path through it.
pub(crate) struct OwnedGroupProcess(Process);

impl OwnedGroupProcess {
    pub(crate) fn pid(&self) -> i32 {
        self.0.pid()
    }

    /// This process through any exec from now ([`Life`]); `None` once it has exited.
    pub(crate) fn life(&self) -> io::Result<Option<Life>> {
        Ok(self.0.record()?.map(|record| record.life()))
    }

    /// `SIGTERM` the exact identity the ledger proved: `Ok(false)` when it no longer runs.
    pub(crate) fn terminate(&self) -> io::Result<bool> {
        self.0.signal(libc::SIGTERM)
    }
}

/// The job's own process `pid`, as the ledger at `path` proves it. The pid must be a member of a
/// group whose [`ownership`] proof holds at this read -- a recorded group, or an unresolved one
/// a lost supervisor left, whose leader still holds its id: the proof is the leader's identity
/// and the membership read, not which list names the group. A group whose leader is gone or
/// belongs to another process names nothing. A ledger that is absent names no job; one that
/// cannot be read is an error, never an empty answer.
pub(crate) fn owned_process(path: &Path, pid: i32) -> io::Result<Option<OwnedGroupProcess>> {
    let Some(ledger) = read(path)? else {
        return Ok(None);
    };
    for group in ledger
        .groups
        .iter()
        .chain(ledger.unresolved.iter().map(|unresolved| &unresolved.group))
    {
        if let Ownership::Owned(members) = ownership(group)?
            && let Some(member) = members.into_iter().find(|member| member.pid() == pid)
        {
            return Ok(Some(OwnedGroupProcess(member)));
        }
    }
    Ok(None)
}

/// How a process's exit was seen ([`Process::exited`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExitObserved {
    /// The exit watch registered while it ran reported its exit.
    Watched,
    /// It had exited, or begun to, before a watch could name it.
    BeforeWatch,
}

impl std::fmt::Display for ExitObserved {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Watched => "its exit watch reported its exit",
            Self::BeforeWatch => "it had exited before it could be watched",
        })
    }
}

impl Process {
    /// The life `pid` names now and its exit, watched from now; `None` once it has exited: a
    /// child that binds itself to a land turn between its fork and its exec is watched across that
    /// exec ([`Life::exit`]).
    pub fn watch_life(pid: i32) -> io::Result<Option<(Life, ProcessExit)>> {
        let Some(life) = Life::of(pid)? else {
            return Ok(None);
        };
        Ok(match life.exit()? {
            ProcessExit::Before => None,
            watching => Some((life, watching)),
        })
    }
}

impl Process {
    /// Watch this process's exit from now, so an exit after this call is never missed.
    pub fn exit(&self) -> io::Result<ProcessExit> {
        Ok(self
            .watch_running()?
            .map_or(ProcessExit::Before, ProcessExit::Watching))
    }
}

/// A process's exit, watched from the moment [`Process::exit`] was asked.
pub enum ProcessExit {
    /// Its exit watch, registered while it ran.
    Watching(ExitWatch),
    /// It had exited, or begun to, before a watch could name it.
    Before,
}

impl ProcessExit {
    /// The exit, awaited without a thread of its own: the watch's descriptor is readable once
    /// the kernel reports the exit (`NOTE_EXIT`, a readable pidfd), never a poll of the process
    /// table.
    pub async fn observed(self) -> io::Result<ExitObserved> {
        let watch = match self {
            Self::Watching(watch) => watch,
            Self::Before => return Ok(ExitObserved::BeforeWatch),
        };
        let watch = tokio::io::unix::AsyncFd::with_interest(watch, tokio::io::Interest::READABLE)?;
        loop {
            let mut ready = watch.readable().await?;
            if ready.get_inner().within(Duration::ZERO)? {
                return Ok(ExitObserved::Watched);
            }
            ready.clear_ready();
        }
    }
}

#[cfg(test)]
mod tests {
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::collections::BTreeSet;
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    use super::{GroupLeader, Writer, end_recorded, ledger_path, names_groups, take_lost};
    use cowshed_os::fork_lock::Spawn as _;

    pub(super) fn leader(pid: u32) -> GroupLeader {
        GroupLeader::observe(pid).expect("observe a live test group leader")
    }

    /// A watch taken on a running process by pid reports it running, then its exit once it is
    /// killed; a pid no process holds any more (reaped) is no watch at all. The reaped process
    /// leads a group a second member keeps alive, and neither kernel gives a live group's id to a
    /// new process, so no process can take the pid between the reap and the second look.
    #[test]
    fn an_exit_watch_by_pid_sees_the_exit_and_none_for_a_reaped_pid() {
        // `/bin/sh` reading the pipe the test holds open runs until it is killed, on every host:
        // NixOS keeps `/bin/sh` and no `/bin/sleep`.
        let shell = |group: i32| {
            Command::new("/bin/sh")
                .stdin(Stdio::piped())
                .process_group(group)
                .spawn_locked()
                .expect("spawn sh")
        };
        let mut leader = OwnedGroup(shell(0));
        let pid = leader.0.id();
        let mut member = shell(i32::try_from(pid).expect("leader pid"));
        let watch = super::ExitWatch::of(pid)
            .expect("watch")
            .expect("a running child is watched");
        assert!(!watch.within(Duration::from_millis(50)).expect("poll"));
        leader.0.kill().expect("kill");
        watch.wait().expect("the exit is reported");
        leader.0.wait_bounded().expect("reap");
        assert!(
            super::ExitWatch::of(pid).expect("watch").is_none(),
            "a reaped pid names no process"
        );
        member.kill().expect("kill the member");
        member.wait_bounded().expect("reap the member");
    }

    /// A job-shaped process tree: a group leader waiting for its child, which blocks reading the
    /// leader's stdin, a pipe the returned leader holds (`sh` would give a background job
    /// `/dev/null`).
    pub(super) fn job_group() -> std::process::Child {
        Command::new("/bin/sh")
            .args(["-c", "exec 3<&0; read _ <&3 & wait"])
            .stdin(Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("spawn a job group")
    }

    /// Taking a stop report never reaps: on a child that already exited the take finds nothing
    /// and leaves the exit pending, so the fence alone collects it, even when the child exits
    /// between the read that met its stop and the take.
    #[test]
    fn taking_a_stop_report_never_reaps_an_exited_child() {
        let mut child = OwnedGroup(
            Command::new("/bin/sh")
                .args(["-c", "exit 4"])
                .stdin(Stdio::null())
                .process_group(0)
                .spawn_locked()
                .expect("spawn a child"),
        );
        let pid = i32::try_from(child.0.id()).expect("a pid");
        assert_eq!(
            super::await_exit_unreaped(pid).expect("exit"),
            crate::api::dto::ExitStatus::Exited { code: 4 }
        );
        super::take_stop_report(libc::id_t::try_from(pid).expect("an id")).expect("take nothing");
        assert_eq!(
            super::exit_unreaped(pid).expect("still unreaped"),
            Some(crate::api::dto::ExitStatus::Exited { code: 4 })
        );
        assert_eq!(child.0.wait_bounded().expect("reap").code(), Some(4));
    }

    /// ECHILD is not success on its own: this live process is never its own child.
    #[test]
    fn taking_a_stop_report_refuses_a_process_that_is_not_our_child() {
        let id = libc::id_t::try_from(std::process::id()).expect("an id");
        let error = super::take_stop_report(id).expect_err("not our child");
        assert_eq!(error.raw_os_error(), Some(libc::ECHILD));
    }

    /// A test's group, killed and its leader reaped however the test ends.
    pub(super) struct OwnedGroup(pub(super) std::process::Child);

    impl Drop for OwnedGroup {
        fn drop(&mut self) {
            let pgid = i32::try_from(self.0.id()).expect("owned child pid");
            // SAFETY: this unreaped child is the group leader spawned by this test.
            unsafe { libc::killpg(pgid, libc::SIGKILL) };
            let _ = self.0.wait_bounded();
        }
    }

    #[test]
    fn recovery_retires_recorded_detached_lives_after_leader_reap_without_pid_guessing() {
        use super::super::process_tree::{BirthToken, ProcessIdentity};
        use std::io::{Read as _, Write as _};

        struct Root(Option<std::process::Child>);
        impl Drop for Root {
            fn drop(&mut self) {
                if let Some(child) = &mut self.0 {
                    // SAFETY: this fixture still holds its unreaped group leader.
                    unsafe {
                        libc::killpg(i32::try_from(child.id()).unwrap(), libc::SIGKILL);
                    }
                    child.wait_bounded().expect("reap fixture root");
                }
            }
        }
        for case in ["exact", "different-life", "different-boot"] {
            let (scratch, ledger) = scratch();
            let socket = scratch.join("child.sock");
            let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
            let script = r#"
import os, signal, socket, struct, sys
if os.fork() == 0:
    os.setsid()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    peer = socket.socket(socket.AF_UNIX)
    peer.connect(sys.argv[1])
    peer.sendall(struct.pack("!II", os.getpid(), os.getppid()))
    peer.recv(1)
    os._exit(0)
os.read(0, 1)
os._exit(0)
"#;
            let mut root = Root(Some(
                Command::new("python3")
                    .args(["-c", script])
                    .arg(&socket)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .stderr(Stdio::inherit())
                    .process_group(0)
                    .spawn_locked()
                    .unwrap(),
            ));
            let parent = root.0.as_ref().unwrap().id();
            let birth = leader(parent);
            let (mut peer, _) = listener.accept().unwrap();
            let mut report = [0_u8; 8];
            peer.read_exact(&mut report).unwrap();
            let child = u32::from_be_bytes(report[..4].try_into().unwrap());
            assert_eq!(u32::from_be_bytes(report[4..].try_into().unwrap()), parent);
            let life = super::process_record(i32::try_from(child).unwrap())
                .unwrap()
                .unwrap();
            let parent_life = super::process_record(i32::try_from(parent).unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(
                (life.ppid, life.parent_unique_id),
                (parent, parent_life.unique_id)
            );
            let exit = super::Process::observe(i32::try_from(child).unwrap())
                .unwrap()
                .unwrap()
                .watch_exit()
                .unwrap();
            super::Recorder::new(ledger.clone())
                .record(
                    [super::JobGroupClaim {
                        job_id: 41,
                        leader: birth,
                        members: Some(
                            vec![ProcessIdentity {
                                pid: child,
                                birth: BirthToken(if case == "different-life" {
                                    life.unique_id + 1
                                } else {
                                    life.unique_id
                                }),
                            }]
                            .into(),
                        ),
                    }]
                    .into_iter(),
                    &[],
                )
                .unwrap();
            if case == "different-boot" {
                let mut recorded = super::read(&ledger).unwrap().unwrap();
                recorded.groups[0].members.as_mut().unwrap().boot_session =
                    "a-different-boot".into();
                super::replace(&ledger, &recorded).unwrap();
            }
            root.0
                .as_mut()
                .unwrap()
                .stdin
                .as_mut()
                .unwrap()
                .write_all(b"x")
                .unwrap();
            assert!(root.0.take().unwrap().wait_bounded().unwrap().success());
            assert!(!super::group_has_live_members(i32::try_from(parent).unwrap()).unwrap());
            assert_eq!(
                super::process_record(i32::try_from(child).unwrap())
                    .unwrap()
                    .unwrap()
                    .unique_id,
                life.unique_id
            );
            let recovered = take_lost(&ledger, Duration::from_millis(20)).unwrap();
            assert!(recovered.is_empty());
            assert!(!ledger.exists());
            let still_live = super::process_record(i32::try_from(child).unwrap())
                .unwrap()
                .is_some_and(|now| now.unique_id == life.unique_id);
            // Closing our fixture capability is the only cleanup for either refused identity.
            drop(peer);
            exit.wait().unwrap();
            assert_eq!(still_live, case != "exact", "{case}");
        }
    }

    /// A life is signalled only under its own kernel token: the same pid under another token is
    /// never reached, and a life that exited -- held unreaped, so its pid names nothing else --
    /// is reached by nothing.
    #[test]
    fn a_life_is_signalled_only_under_its_own_unique_id() {
        let group = OwnedGroup(job_group());
        let pid = group.0.id();
        let leader_pid = i32::try_from(pid).unwrap();
        let records = super::job_member_records(&super::Birth::Observed(leader(pid))).unwrap();
        let ours = records
            .iter()
            .find(|record| record.pid == pid)
            .copied()
            .expect("the running leader is a member of its group");
        assert!(!super::signal_life(pid, ours.unique_id.wrapping_add(1), libc::SIGTERM).unwrap());
        assert!(
            super::process_record(leader_pid)
                .unwrap()
                .is_some_and(|now| now.unique_id == ours.unique_id),
            "a signal under another unique id reached the leader"
        );
        assert!(super::signal_life(pid, ours.unique_id, libc::SIGTERM).unwrap());
        assert_eq!(
            super::await_exit_unreaped(leader_pid).unwrap(),
            crate::api::dto::ExitStatus::Signaled {
                signal: libc::SIGTERM,
                core_dumped: false,
            }
        );
        assert!(!super::signal_life(pid, ours.unique_id, libc::SIGKILL).unwrap());
    }

    /// A group led by `sh` whose child is `script`'s last background job: the leader, held
    /// unreaped by the test, and the child's pid, which `script` prints. The leader's stdin is a
    /// pipe the returned leader holds open, for a child that waits on it.
    pub(super) fn group_with_child(script: &str) -> (std::process::Child, u32) {
        use std::io::BufRead as _;

        let mut child = Command::new("/bin/sh")
            .args(["-c", script])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("spawn a job group");
        let mut line = String::new();
        std::io::BufReader::new(child.stdout.take().expect("piped stdout"))
            .read_line(&mut line)
            .expect("the group's child pid");
        (child, line.trim().parse().expect("a pid"))
    }

    fn sorted(mut pids: Vec<u32>) -> Vec<u32> {
        pids.sort_unstable();
        pids
    }

    fn pids(members: Vec<super::Process>) -> Vec<u32> {
        sorted(
            members
                .iter()
                .map(|member| u32::try_from(member.pid()).expect("a pid"))
                .collect(),
        )
    }

    #[test]
    fn a_job_s_members_are_its_whole_running_group_and_none_once_it_ended() {
        let (mut job, sleeper) = group_with_child("exec 3<&0; read _ <&3 & echo $!; wait");
        let birth = super::Birth::of(job.id());
        assert_eq!(
            pids(super::job_members(&birth).expect("members")),
            sorted(vec![job.id(), sleeper])
        );

        // SAFETY: plain kill of the test's own grandchild, which is still running.
        assert_eq!(
            unsafe { libc::kill(i32::try_from(sleeper).unwrap(), libc::SIGKILL) },
            0
        );
        // The leader's `wait` returns once its child died, and the leader exits.
        super::await_exit_unreaped(i32::try_from(job.id()).unwrap()).expect("leader exit");
        assert_eq!(
            pids(super::job_members(&birth).expect("an exited, unreaped leader proves its group")),
            Vec::<u32>::new()
        );
        job.wait_bounded().expect("reap the leader");
        assert_eq!(
            pids(super::job_members(&birth).expect("an id nothing holds names an empty group")),
            Vec::<u32>::new()
        );
    }

    /// A member's resident memory is read while it runs; once it has exited -- reaped, or
    /// exited and not yet reaped -- it holds nothing, and that is no error.
    ///
    /// The child prints its own pid, so it has run before the read: a child forked and not yet
    /// run, or one in the middle of an exec, truly holds no resident page yet (measured on macOS
    /// under load: 4 of 300 reads of `sleep 300 &` straight after `echo $!` were 0). It then holds
    /// in `sh`'s own `read` with no exec after, on the leader's stdin, which `sh` would otherwise
    /// give a background job as `/dev/null`.
    #[test]
    fn a_member_s_resident_memory_is_read_while_it_runs_and_none_once_it_exited() {
        let (job, sleeper) =
            group_with_child("exec 3<&0\n/bin/sh -c 'echo $$; read line' <&3 &\nwait");
        let job = OwnedGroup(job);
        let birth = super::Birth::of(job.0.id());
        let members = super::job_members(&birth).expect("members");
        assert_eq!(members.len(), 2, "the leader and its child");
        for member in &members {
            let resident = member.resident_bytes().expect("a running member's memory");
            assert!(
                resident.is_some_and(|bytes| bytes > 0),
                "process {} runs, so it holds memory: {resident:?}",
                member.pid()
            );
        }

        // SAFETY: plain kill of the test's own grandchild, which is still running.
        assert_eq!(
            unsafe { libc::kill(i32::try_from(sleeper).unwrap(), libc::SIGKILL) },
            0
        );
        // The leader reaped its child, then exited; this test holds it unreaped.
        super::await_exit_unreaped(i32::try_from(job.0.id()).unwrap()).expect("leader exit");
        for member in &members {
            assert_eq!(
                member
                    .resident_bytes()
                    .expect("an exited member is no error"),
                None,
                "process {} has exited",
                member.pid()
            );
        }
        // Ending the emptied group reaps its leader.
        drop(job);
    }

    #[test]
    fn members_nothing_proves_the_job_s_are_an_error_never_a_membership() {
        // The leader exits at once and is reaped, while its child holds the group id on.
        let (mut job, sleeper) = group_with_child("exec 3<&0; read _ <&3 & echo $!");
        let birth = super::Birth::of(job.id());
        // The child reads this pipe: reaping the leader must not close it.
        let held = job.stdin.take();
        job.wait_bounded().expect("reap the leader");
        let orphaned = super::job_members(&birth);
        // SAFETY: plain kill of the test's own orphaned grandchild.
        unsafe { libc::kill(i32::try_from(sleeper).unwrap(), libc::SIGKILL) };
        drop(held);
        assert!(orphaned.is_err(), "{orphaned:?}");

        let mut group = job_group();
        let unidentified = super::Birth::Unobserved {
            pid: group.id(),
            reason: "a test that identified nothing".into(),
        };
        let members = super::job_members(&unidentified);
        let pgid = i32::try_from(group.id()).unwrap();
        // SAFETY: this unreaped child leads the group spawned by this test.
        unsafe { libc::killpg(pgid, libc::SIGKILL) };
        group.wait_bounded().expect("reap the group leader");
        assert!(members.is_err(), "{members:?}");
    }

    #[test]
    fn an_absent_leader_does_not_hide_its_live_group_descendant() {
        use std::io::BufRead as _;

        let child = Command::new("/bin/sh")
            .args([
                "-c",
                "exec 3<&0; (trap '' TERM; echo READY; read _ <&3) & wait",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .unwrap();
        let mut group = OwnedGroup(child);
        let pgid = i32::try_from(group.0.id()).unwrap();
        let mut ready = String::new();
        std::io::BufReader::new(group.0.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        assert_eq!(ready.trim(), "READY");
        let (_scratch, ledger) = scratch();
        super::Recorder::new(ledger.clone())
            .record(
                [super::JobGroupClaim::group(9, leader(group.0.id()))].into_iter(),
                &[],
            )
            .unwrap();
        group.0.kill().unwrap();
        // The leader exited; this test holds it unreaped, so its start time still answers.
        super::await_exit_unreaped(pgid).expect("leader exit");
        assert!(super::start_time(pgid).unwrap().is_some());
        assert!(super::group_has_live_members(pgid).unwrap());
        assert_eq!(
            end_recorded(&ledger, Writer::Any, Duration::ZERO)
                .unwrap()
                .signalled,
            vec![9]
        );
        drop(group);
        while super::group_has_live_members(pgid).unwrap() {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!super::group_has_live_members(pgid).unwrap());
        assert!(take_lost(&ledger, Duration::ZERO).unwrap().is_empty());
        assert!(!ledger.exists());
    }

    pub(super) fn group_alive(pgid: i32) -> bool {
        // SAFETY: signal 0 only checks that the group exists.
        unsafe { libc::killpg(pgid, 0) == 0 }
    }

    /// A scratch ledger and the directory holding it. The ledger's own removal is under test; the
    /// directory goes when the test ends, on any path.
    pub(super) fn scratch() -> (crate::temp_root::TempRoot, std::path::PathBuf) {
        let directory = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "cowshed-groups",
        );
        let ledger = ledger_path(&directory.join("s.sock"));
        (directory, ledger)
    }

    #[test]
    fn a_lost_supervisor_s_groups_end_whole_and_its_ledger_goes() {
        let mut job = job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let (_scratch, ledger) = scratch();
        super::Recorder::new(ledger.clone())
            .record(
                [super::JobGroupClaim::group(7, leader(job.id()))].into_iter(),
                &[],
            )
            .unwrap();
        assert!(group_alive(pgid));
        // The child reads this pipe: reaping the leader must not end it, only the signals may.
        let held = job.stdin.take();

        let ended = end_recorded(&ledger, Writer::Any, Duration::from_millis(200)).unwrap();
        assert_eq!(ended.signalled, vec![7]);
        let _ = job.wait_bounded();
        while group_alive(pgid) {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!group_alive(pgid), "the leader's child died with it");
        drop(held);
        assert_eq!(
            end_recorded(&ledger, Writer::Any, Duration::ZERO).unwrap(),
            super::Ended::default(),
            "an ended ledger ends nothing twice"
        );
        assert!(take_lost(&ledger, Duration::ZERO).unwrap().is_empty());
        assert!(!ledger.exists());
    }

    #[test]
    fn a_watcher_of_one_supervisor_never_acts_on_a_newer_one_s_ledger() {
        let mut job = job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let (_scratch, ledger) = scratch();
        super::Recorder::new(ledger.clone())
            .record(
                [super::JobGroupClaim::group(3, leader(job.id()))].into_iter(),
                &[],
            )
            .unwrap();

        let other_supervisor = std::process::id() + 1;
        let ended =
            end_recorded(&ledger, Writer::Process(other_supervisor), Duration::ZERO).unwrap();
        assert_eq!(ended, super::Ended::default());
        assert!(
            group_alive(pgid),
            "a newer supervisor's running job is left alone"
        );
        assert!(ledger.exists());

        end_recorded(
            &ledger,
            Writer::Process(std::process::id()),
            Duration::from_millis(100),
        )
        .unwrap();
        let _ = job.wait_bounded();
    }

    #[test]
    fn an_idle_census_observes_live_jobs_without_signalling_or_rewriting() {
        use std::io::{BufRead as _, Write as _};
        let (_scratch, path) = scratch();
        assert_eq!(super::live_job(&path).unwrap(), None);
        let mut job = OwnedGroup(
            Command::new("sh")
                .args(["-c", "while read line; do printf '%s\\n' \"$line\"; done"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .process_group(0)
                .spawn_locked()
                .expect("pipe-held job"),
        );
        let mut output = std::io::BufReader::new(job.0.stdout.take().unwrap());
        super::Recorder::new(path.clone())
            .record(
                [super::JobGroupClaim::group(4, leader(job.0.id()))].into_iter(),
                &[],
            )
            .unwrap();
        for inherited in [false, true] {
            if inherited {
                let mut ledger = super::read(&path).unwrap().unwrap();
                let group = ledger.groups.pop().unwrap();
                ledger.unresolved.push(super::UnresolvedGroup {
                    lost_supervisor: ledger.supervisor,
                    group,
                });
                super::replace(&path, &ledger).unwrap();
            }
            let before = std::fs::read(&path).unwrap();
            assert_eq!(super::live_job(&path).unwrap(), Some(4));
            assert_eq!(
                std::fs::read(&path).unwrap(),
                before,
                "inspection rewrites nothing"
            );
            job.0
                .stdin
                .as_mut()
                .unwrap()
                .write_all(b"still alive\n")
                .unwrap();
            let mut response = String::new();
            output.read_line(&mut response).unwrap();
            assert_eq!(
                response, "still alive\n",
                "the census stopped no recorded job"
            );
        }
        drop(job);
        assert_eq!(super::live_job(&path).unwrap(), None);
    }
    /// A supervisor that retired in order left a ledger naming no group: its watcher has nothing
    /// to recover. One that left a group does, and only its own watcher, or a holder of the
    /// workspace's socket, may act on it.
    #[test]
    fn only_a_ledger_naming_the_watched_supervisor_s_groups_needs_recovery() {
        let (_scratch, ledger) = scratch();
        let own = Writer::Process(std::process::id());
        assert!(!names_groups(&ledger, Writer::Any).unwrap(), "no ledger");
        super::Recorder::new(ledger.clone())
            .record(std::iter::empty(), &[])
            .unwrap();
        assert!(!names_groups(&ledger, own).unwrap(), "retired in order");

        let mut job = job_group();
        super::Recorder::new(ledger.clone())
            .record(
                [super::JobGroupClaim::group(4, leader(job.id()))].into_iter(),
                &[],
            )
            .unwrap();
        assert!(names_groups(&ledger, own).unwrap());
        assert!(names_groups(&ledger, Writer::Any).unwrap());
        assert!(!names_groups(&ledger, Writer::Process(std::process::id() + 1)).unwrap());

        end_recorded(&ledger, own, Duration::from_millis(100)).unwrap();
        let _ = job.wait_bounded();
    }

    /// A byte-identical cut skips the temp+rename rewrite (keeping the same inode), while a cut
    /// with distinct serialized bytes writes a fresh file. A failed write never poisons the last
    /// published bytes, so retrying the same cut once the fault clears still writes.
    #[test]
    fn recorder_skips_byte_identical_cuts_and_writes_on_change_or_after_failure() {
        use std::os::unix::fs::MetadataExt as _;

        let (_scratch, ledger) = scratch();
        let recorder = super::Recorder::new(ledger.clone());
        let first = GroupLeader::from_parts(42, 1, 100).unwrap();
        let second = GroupLeader::from_parts(43, 2, 200).unwrap();

        recorder
            .record([super::JobGroupClaim::group(1, first)].into_iter(), &[])
            .unwrap();
        let first_inode = std::fs::symlink_metadata(&ledger).unwrap().ino();

        recorder
            .record([super::JobGroupClaim::group(1, first)].into_iter(), &[])
            .unwrap();
        assert_eq!(
            std::fs::symlink_metadata(&ledger).unwrap().ino(),
            first_inode,
            "a byte-identical cut skips temp+rename and keeps the existing inode"
        );

        recorder
            .record([super::JobGroupClaim::group(2, second)].into_iter(), &[])
            .unwrap();
        let second_inode = std::fs::symlink_metadata(&ledger).unwrap().ino();
        assert_ne!(
            second_inode, first_inode,
            "a changed cut publishes a new file"
        );

        // Block the temporary publication path with a directory so the next distinct cut fails,
        // then clear the fault and verify the same cut is not falsely skipped.
        let temporary = ledger.with_extension(format!("groups.{}", std::process::id()));
        std::fs::create_dir(&temporary).unwrap();
        let third = GroupLeader::from_parts(44, 3, 300).unwrap();
        recorder
            .record([super::JobGroupClaim::group(3, third)].into_iter(), &[])
            .expect_err("a directory at the temporary path fails publication");
        std::fs::remove_dir(&temporary).unwrap();

        recorder
            .record([super::JobGroupClaim::group(3, third)].into_iter(), &[])
            .unwrap();
        assert_ne!(
            std::fs::symlink_metadata(&ledger).unwrap().ino(),
            second_inode,
            "a failed write does not cache its bytes; retrying the same cut publishes"
        );
    }

    /// A ledger rewrite carries the identity a group's leader had when the job started. Here
    /// the job's pid now names another live group leader -- what a pid reuse looks like from the
    /// ledger -- and recording it again must not adopt that stranger as the job's group, nor may
    /// ending the ledger signal it.
    #[test]
    fn a_rewrite_never_adopts_a_process_that_reused_the_job_s_pid() {
        fn end_fixture(child: &mut std::process::Child) {
            let pgid = i32::try_from(child.id()).unwrap();
            let exits: Vec<_> = super::members_of(pgid)
                .unwrap()
                .iter()
                .map(|member| member.watch_exit().unwrap())
                .collect();
            // SAFETY: the unreaped test child leads this group; both members are pipe-held.
            assert_eq!(unsafe { libc::killpg(pgid, libc::SIGKILL) }, 0);
            child.wait_bounded().unwrap();
            for exit in exits {
                assert!(exit.within(Duration::from_secs(2)).unwrap());
            }
        }

        let (mut gone, _) =
            group_with_child("exec 3<&0\n/bin/sh -c 'echo $$; read line' <&3 &\nwait");
        let first_seen = leader(gone.id());
        end_fixture(&mut gone);

        let (mut stranger, _) =
            group_with_child("exec 3<&0\n/bin/sh -c 'echo $$; read line' <&3 &\nwait");
        let stranger_pgid = i32::try_from(stranger.id()).unwrap();
        let stranger_leader = leader(stranger.id());
        // The job started before the stranger that took its pid. Linux counts starts in clock
        // ticks, so a job killed and replaced within one tick shares the stranger's start there;
        // a job that truly held this pid earlier started at least one tick sooner.
        let reused = GroupLeader {
            pgid: stranger_pgid,
            birth: first_seen.birth.min(stranger_leader.birth - 1),
            start: first_seen.start.min(stranger_leader.start - 1),
        };
        assert_ne!(reused, stranger_leader);
        let (_scratch, ledger) = scratch();
        super::Recorder::new(ledger.clone())
            .record([super::JobGroupClaim::group(5, reused)].into_iter(), &[])
            .unwrap();
        super::Recorder::new(ledger.clone())
            .record([super::JobGroupClaim::group(5, reused)].into_iter(), &[])
            .unwrap();

        assert_eq!(
            end_recorded(&ledger, Writer::Any, Duration::ZERO).unwrap(),
            super::Ended::default()
        );
        assert!(take_lost(&ledger, Duration::ZERO).unwrap().is_empty());
        assert!(
            group_alive(stranger_pgid),
            "the stranger's group was signalled"
        );
        end_fixture(&mut stranger);
    }

    /// Once a group's leader is reaped, the processes holding its id cannot be told from a
    /// stranger's group that took the id after the job's group ended: here a group whose leader
    /// exited and was reaped, leaving a descendant that holds the id. Whatever identity the ledger
    /// recorded -- this build's start time, an earlier build's birth time, or none -- recovery
    /// never signals it. The next supervisor takes it over as unresolved and every later ledger
    /// carries it until nothing holds the id; only then is its record dropped.
    #[test]
    fn a_group_whose_recorded_leader_was_reaped_is_carried_never_signalled() {
        use std::io::BufRead as _;

        type Recorded = fn(i32) -> (Option<u64>, Option<u64>);
        let identities: [(&str, Recorded); 3] = [
            ("start", |pgid| (super::start_time(pgid).unwrap(), None)),
            ("legacy birth", |pgid| {
                (None, super::birth_time(pgid).unwrap())
            }),
            ("none", |_| (None, None)),
        ];
        for (label, recorded) in identities {
            let child = Command::new("/bin/sh")
                .args([
                    "-c",
                    "exec 3<&0; (trap '' TERM; echo READY; read _ <&3) & wait",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .process_group(0)
                .spawn_locked()
                .unwrap();
            let mut group = OwnedGroup(child);
            let pgid = i32::try_from(group.0.id()).unwrap();
            let mut ready = String::new();
            std::io::BufReader::new(group.0.stdout.take().unwrap())
                .read_line(&mut ready)
                .unwrap();
            assert_eq!(ready.trim(), "READY");
            let (leader_start, leader_birth) = recorded(pgid);
            assert_eq!(
                (leader_start.is_some(), leader_birth.is_some()),
                (label == "start", label == "legacy birth"),
                "{label}: recorded while the leader ran"
            );
            // The descendant reads this pipe: reaping the leader must not close it.
            let held = group.0.stdin.take();
            group.0.kill().unwrap();
            group.0.wait_bounded().unwrap();
            assert_eq!(super::start_time(pgid).unwrap(), None, "{label}");
            assert!(super::group_has_live_members(pgid).unwrap(), "{label}");

            let (_scratch, ledger) = scratch();
            let lost_supervisor = std::process::id() + 1;
            super::replace(
                &ledger,
                &super::Ledger {
                    supervisor: lost_supervisor,
                    ended: false,
                    groups: vec![super::Group {
                        job_id: 4,
                        pgid,
                        leader_start,
                        leader_birth,
                        members: None,
                    }],
                    unresolved: Vec::new(),
                },
            )
            .unwrap();
            assert_eq!(
                end_recorded(&ledger, Writer::Any, Duration::ZERO).unwrap(),
                super::Ended {
                    signalled: Vec::new(),
                    unresolved: vec![4],
                },
                "{label}"
            );
            let taken = take_lost(&ledger, Duration::ZERO).unwrap();
            assert_eq!(
                taken
                    .iter()
                    .map(|group| (group.job_id(), group.pgid(), group.lost_supervisor()))
                    .collect::<Vec<_>>(),
                vec![(4, pgid, lost_supervisor)],
                "{label}"
            );
            assert!(group_alive(pgid), "{label}: the group was signalled");
            // The next supervisor carries it in every ledger it writes, and a later recovery
            // takes it over again rather than forgetting it.
            assert_eq!(super::unresolved(&ledger).unwrap(), taken, "{label}");
            assert_eq!(
                super::Recorder::new(ledger.clone())
                    .record(std::iter::empty(), &taken)
                    .unwrap()
                    .carried,
                taken,
                "{label}"
            );
            assert_eq!(
                take_lost(&ledger, Duration::ZERO).unwrap(),
                taken,
                "{label}"
            );
            assert!(group_alive(pgid), "{label}: the group was signalled");

            drop(group);
            drop(held);
            while super::group_has_live_members(pgid).unwrap() {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(super::unresolved(&ledger).unwrap().is_empty(), "{label}");
            assert!(
                super::Recorder::new(ledger.clone())
                    .record(std::iter::empty(), &taken)
                    .unwrap()
                    .carried
                    .is_empty(),
                "{label}"
            );
            assert!(
                take_lost(&ledger, Duration::ZERO).unwrap().is_empty(),
                "{label}"
            );
            assert!(!ledger.exists(), "{label}");
        }
    }

    /// Set in the environment of this test binary run as a [`listener_fixture_role`].
    const LISTENER_ROLE: &str = "COWSHED_LISTENER_FIXTURE";

    /// Not a test of its own. Run as a process of a [`ListeningGroup`] with [`LISTENER_ROLE`]
    /// set, it holds every kind of socket a port read must tell apart -- a dual-stack IPv6
    /// listener and an IPv4 listener sharing its port, a second dual-stack listener, a TCP
    /// socket bound but not listening, both ends of a connected TCP socket and a bound UDP
    /// socket -- prints them as a [`Fixture`] line and waits to be killed.
    ///
    /// Every TCP port here is first taken on the dual-stack wildcard `[::]`, which holds it in
    /// both families, so no other socket of this fixture, of another fixture or of the test
    /// shares its number: a port named is named for exactly one socket. The IPv4 listener joins
    /// its dual-stack twin's port through `SO_REUSEPORT`, which both set.
    #[test]
    fn listener_fixture_role() {
        if std::env::var_os(LISTENER_ROLE).is_none() {
            return;
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("a runtime for the fixture's sockets");
        let _entered = runtime.enter();
        let wildcard = || std::net::SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, 0));
        let shared = tokio::net::TcpSocket::new_v6().expect("dual-stack socket");
        shared
            .set_reuseport(true)
            .expect("share the dual-stack port");
        shared
            .bind(wildcard())
            .expect("bind the dual-stack listener");
        let shared = shared.listen(8).expect("dual-stack listener");
        let port = shared.local_addr().expect("dual-stack address").port();
        let four = tokio::net::TcpSocket::new_v4().expect("IPv4 socket");
        four.set_reuseport(true).expect("share the port");
        four.bind(std::net::SocketAddr::from((
            std::net::Ipv4Addr::LOCALHOST,
            port,
        )))
        .expect("bind the IPv4 listener at the dual-stack port");
        let _four = four.listen(8).expect("IPv4 listener");
        let other = std::net::TcpListener::bind(wildcard()).expect("second dual-stack listener");
        let bound = tokio::net::TcpSocket::new_v6().expect("TCP socket");
        bound.bind(wildcard()).expect("bind without listening");
        let client = tokio::net::TcpSocket::new_v6().expect("TCP client socket");
        client.bind(wildcard()).expect("bind the client");
        let connected = runtime
            .block_on(client.connect(std::net::SocketAddr::from((
                std::net::Ipv6Addr::LOCALHOST,
                port,
            ))))
            .expect("connect")
            .into_std()
            .expect("the connected socket");
        let _accepted = runtime.block_on(shared.accept()).expect("accept");
        let udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("UDP socket");
        println!(
            "fixture {} {} {port} {} {} {} {}",
            std::process::id(),
            std::os::unix::process::parent_id(),
            other.local_addr().expect("second listener address").port(),
            bound.local_addr().expect("bound address").port(),
            connected.local_addr().expect("connected address").port(),
            udp.local_addr().expect("UDP address").port(),
        );
        loop {
            std::thread::park();
        }
    }

    /// What a [`listener_fixture_role`] holds, as it printed it.
    #[derive(Debug)]
    struct Fixture {
        pid: u32,
        parent: u32,
        /// The port its dual-stack listener and its IPv4 listener share.
        shared: u16,
        /// The port of its second dual-stack listener.
        other: u16,
        /// The port of its TCP socket that is bound and does not listen.
        bound: u16,
        /// The local port of its connected TCP socket's client end.
        connected: u16,
    }

    /// A job-shaped group whose grandchild is a [`listener_fixture_role`]: the leader `sh` runs a
    /// child `sh`, which runs the fixture. Dropping it ends a group whose leader is still
    /// unreaped.
    struct ListeningGroup {
        leader: std::process::Child,
        _output: std::io::BufReader<std::process::ChildStdout>,
    }

    impl Drop for ListeningGroup {
        fn drop(&mut self) {
            // A reaped leader no longer proves the group id the test's own.
            if let Ok(None) = self.leader.try_wait() {
                let pgid = i32::try_from(self.leader.id()).expect("owned child pid");
                // SAFETY: this unreaped child leads the group this test spawned.
                unsafe { libc::killpg(pgid, libc::SIGKILL) };
                let _ = self.leader.wait_bounded();
            }
        }
    }

    fn listening_group() -> (ListeningGroup, Fixture) {
        use std::io::BufRead as _;

        let mut leader = Command::new("/bin/sh")
            .args([
                "-c",
                r#""$@" & wait"#,
                "sh",
                "/bin/sh",
                "-c",
                r#""$@" & wait"#,
                "sh",
            ])
            .arg(std::env::current_exe().expect("this test binary"))
            .args([
                "--exact",
                "runtime::job_groups::tests::listener_fixture_role",
                "--nocapture",
                "--quiet",
            ])
            .env(LISTENER_ROLE, "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("spawn a listening group");
        let mut output =
            std::io::BufReader::new(leader.stdout.take().expect("the group's piped stdout"));
        let mut line = String::new();
        // Quiet libtest output keeps its single-thread test-name prefix off the fixture line.
        while !line.starts_with("fixture ") {
            line.clear();
            let read = output.read_line(&mut line).expect("the fixture's line");
            assert_ne!(read, 0, "the fixture ended before it printed its sockets");
        }
        let fields: Vec<u32> = line
            .split_whitespace()
            .skip(1)
            .map(|field| field.parse().expect("a number"))
            .collect();
        let port = |index: usize| u16::try_from(fields[index]).expect("a port");
        let fixture = Fixture {
            pid: fields[0],
            parent: fields[1],
            shared: port(2),
            other: port(3),
            bound: port(4),
            connected: port(5),
        };
        (
            ListeningGroup {
                leader,
                _output: output,
            },
            fixture,
        )
    }

    /// A job's listening ports are the TCP listeners its group's processes hold -- a grandchild's
    /// included, IPv4 and IPv6 alike, each port once -- and nothing else: not a bound or a
    /// connected socket, not this test's own listener, not another job's. Once the job ended it
    /// listens on nothing.
    #[test]
    fn a_job_s_listening_ports_are_its_group_s_tcp_listeners_and_none_once_it_ended() {
        // Bound to the dual-stack wildcard, its port is no fixture listener's (see the fixture).
        let host = std::net::TcpListener::bind("[::]:0").expect("host listener");
        let unrelated = host.local_addr().expect("host listener address").port();
        let (mut job, fixture) = listening_group();
        let (other, other_fixture) = listening_group();
        assert_ne!(
            fixture.parent,
            job.leader.id(),
            "the fixture is the job's grandchild, not its child"
        );

        let birth = super::Birth::of(job.leader.id());
        let ports = super::job_listening_ports(&birth).expect("the job's listeners");
        assert_eq!(
            ports,
            BTreeSet::from([fixture.shared, fixture.other]),
            "{fixture:?}"
        );
        for stranger in [
            unrelated,
            fixture.bound,
            fixture.connected,
            other_fixture.shared,
            other_fixture.other,
        ] {
            assert!(!ports.contains(&stranger), "{stranger} in {ports:?}");
        }
        assert_eq!(
            super::job_listening_ports(&super::Birth::of(other.leader.id()))
                .expect("the other job's listeners"),
            BTreeSet::from([other_fixture.shared, other_fixture.other])
        );

        // SAFETY: plain kill of the test's own grandchild, which is still running.
        let fixture_pid = i32::try_from(fixture.pid).expect("fixture pid");
        assert_eq!(unsafe { libc::kill(fixture_pid, libc::SIGKILL) }, 0);
        // The child `sh` ends once the fixture did, and the leader once the child did.
        super::await_exit_unreaped(i32::try_from(job.leader.id()).expect("leader pid"))
            .expect("leader exit");
        assert_eq!(
            super::job_listening_ports(&birth).expect("an exited, unreaped leader's group"),
            BTreeSet::new()
        );
        job.leader.wait_bounded().expect("reap the leader");
        assert_eq!(
            super::job_listening_ports(&birth).expect("an id nothing holds"),
            BTreeSet::new()
        );
    }

    /// A member's listeners are read while it runs; once it exited it holds none, and that is no
    /// error. Its group's other processes hold none at all.
    #[test]
    fn a_member_s_listeners_are_read_while_it_runs_and_none_once_it_exited() {
        let (job, fixture) = listening_group();
        let members = super::job_members(&super::Birth::of(job.leader.id())).expect("members");
        assert_eq!(members.len(), 3, "the leader, its child and the fixture");
        let fixture_pid = i32::try_from(fixture.pid).expect("fixture pid");
        for member in &members {
            let expected = if member.pid() == fixture_pid {
                BTreeSet::from([fixture.shared, fixture.other])
            } else {
                BTreeSet::new()
            };
            assert_eq!(
                member
                    .listening_ports()
                    .expect("a running member's sockets"),
                Some(expected),
                "process {}",
                member.pid()
            );
        }
        let listener = members
            .iter()
            .find(|member| member.pid() == fixture_pid)
            .expect("the fixture is a member");
        let exited = listener.watch_exit().expect("watch the fixture");
        // SAFETY: plain kill of the test's own grandchild, which is still running.
        assert_eq!(unsafe { libc::kill(fixture_pid, libc::SIGKILL) }, 0);
        exited.wait().expect("the fixture exits");
        assert_eq!(
            listener
                .listening_ports()
                .expect("an exited member is no error"),
            None
        );
        drop(job);
    }
}

#[cfg(test)]
#[path = "job_groups_publication_tests.rs"]
mod publication_tests;
