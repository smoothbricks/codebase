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

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use cowshed_os::process::ExitWatch;

use super::process_tree::ProcessIdentity;
use crate::api::dto::ExitStatus;

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

/// Observed Darwin identities are unique within one boot, never across a reboot.
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
    let session = cowshed_apfs::attach_budget::boot()?.session;
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

/// A different boot proves the recorded Darwin lives ended, without consulting recycled pids.
#[cfg(target_os = "macos")]
fn recorded_members_alive(group: &Group) -> io::Result<bool> {
    let Some(members) = &group.members else {
        return Ok(false);
    };
    if members.boot_session != cowshed_apfs::attach_budget::boot()?.session {
        return Ok(false);
    }
    for life in &members.lives {
        if recorded_life(*life)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(not(target_os = "macos"))]
fn recorded_members_alive(group: &Group) -> io::Result<bool> {
    if group
        .members
        .as_ref()
        .is_some_and(|members| !members.lives.is_empty())
    {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "this kernel cannot inspect recorded Darwin process identities",
        ));
    }
    Ok(false)
}

#[cfg(target_os = "macos")]
fn recorded_life(life: ProcessIdentity) -> io::Result<Option<Process>> {
    let pid = i32::try_from(life.pid).map_err(io::Error::other)?;
    if pid <= 0 || life.birth.0 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "recorded process life needs a positive pid and unique id",
        ));
    }
    Ok(running_identity(pid)?
        .filter(|identity| identity.unique_id == life.birth.0)
        .map(|identity| Process {
            pid,
            version: identity.id_version.cast_unsigned(),
        }))
}

#[cfg(target_os = "macos")]
fn end_recorded_members(groups: &[Group], grace: Duration) -> io::Result<Vec<u64>> {
    if groups.iter().all(|group| group.members.is_none()) {
        return Ok(Vec::new());
    }
    let boot = cowshed_apfs::attach_budget::boot()?.session;
    let mut watched = Vec::new();
    let mut signalled = BTreeSet::new();
    for group in groups {
        let Some(members) = group
            .members
            .as_ref()
            .filter(|members| members.boot_session == boot)
        else {
            continue;
        };
        for &life in &members.lives {
            let deadline = std::time::Instant::now() + Duration::from_secs(1);
            while let Some(process) = recorded_life(life)? {
                match process.watch_exit() {
                    Ok(exit) => {
                        watched.push((group.job_id, life, exit, false));
                        break;
                    }
                    Err(error) => {
                        let current = recorded_life(life)?;
                        match current {
                            None => break,
                            Some(now)
                                if now.version != process.version
                                    && std::time::Instant::now() < deadline =>
                            {
                                continue;
                            }
                            Some(_) => return Err(error),
                        }
                    }
                }
            }
        }
    }
    for (job, life, _, _) in &watched {
        if signal_life(life.pid, life.birth.0, libc::SIGTERM)? {
            signalled.insert(*job);
        }
    }
    let deadline = std::time::Instant::now() + grace;
    for (_, _, exit, ended) in &mut watched {
        *ended = exit.within(deadline.saturating_duration_since(std::time::Instant::now()))?;
    }
    for (job, life, _, ended) in &watched {
        if !ended && signal_life(life.pid, life.birth.0, libc::SIGKILL)? {
            signalled.insert(*job);
        }
    }
    let deadline = std::time::Instant::now() + grace.max(Duration::from_secs(1));
    for (_, life, exit, ended) in watched {
        if !ended && !exit.within(deadline.saturating_duration_since(std::time::Instant::now()))? {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "recorded process {} (unique id {}) has no exit proof after SIGKILL; its ledger claim is retained",
                    life.pid, life.birth.0
                ),
            ));
        }
    }
    Ok(signalled.into_iter().collect())
}

#[cfg(not(target_os = "macos"))]
fn end_recorded_members(groups: &[Group], _grace: Duration) -> io::Result<Vec<u64>> {
    for group in groups {
        recorded_members_alive(group)?;
    }
    Ok(Vec::new())
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

/// [`job_members`]'s membership as the kernel records each member: its pid, parent and the
/// unique ids no other process is given, read under the same held-leader proof.
#[cfg(target_os = "macos")]
pub(crate) fn job_member_records(leader: &Birth) -> io::Result<Vec<ProcessRecord>> {
    proven_members(leader, |pgid| {
        Ok(running_in_group(pgid)?
            .into_iter()
            .map(|life| life.record)
            .collect())
    })
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

/// A process held by an identity the kernel never gives another: its pid and that pid's version,
/// as an audit token carries them. Signalling it reaches that process or nothing.
#[cfg(target_os = "macos")]
#[derive(Debug)]
pub struct Process {
    pid: i32,
    version: u32,
}

/// What a signal `proc_signal_with_audittoken` refused with `EPERM` means ([`Process::signal`]).
#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PermissionRefusal {
    /// No process with the signalled identity runs: the token named a life that has ended.
    Gone,
    /// The process with the signalled identity runs and the kernel denied the signal.
    Denied,
}

/// [`PermissionRefusal`] from `running`, the pid version of the life the pid holds as read after
/// the refusal. The kernel refuses an audit token with `EPERM` not only when it denies the signal
/// but also when the token names no running process (its process exited, or its pid now holds
/// another life), so the refusal is a denial only while the life with `version` still runs. A read
/// that fails proves nothing gone: it stays a denial.
#[cfg(target_os = "macos")]
fn permission_refusal(version: u32, running: Result<Option<u32>, &io::Error>) -> PermissionRefusal {
    match running {
        Ok(Some(running)) if running == version => PermissionRefusal::Denied,
        Ok(_) => PermissionRefusal::Gone,
        Err(_) => PermissionRefusal::Denied,
    }
}

/// An observation after a denied signal, never an identity renewed for a retry.
#[cfg(target_os = "macos")]
#[derive(Debug, thiserror::Error)]
#[error(
    "signaling process {pid} with signal {signal} failed: {source}; expected_version={expected_version}, caller_euid={caller_euid}, post_failure(uid,flags,version)={post_failure:?}, exit_watch=unavailable (none held at the signal boundary)"
)]
struct SignalPermissionDenied {
    pid: i32,
    signal: libc::c_int,
    expected_version: u32,
    caller_euid: u32,
    post_failure: io::Result<Option<(u32, u32, u32)>>,
    source: io::Error,
}

/// `struct proc_uniqidentifierinfo` (`<sys/proc_info.h>`), which the `libc` crate does not
/// declare: `p_uuid[16]`, `p_uniqueid`, `p_puniqueid`, then `p_idversion`, then 20 reserved
/// bytes. The unique ids are never given to another process while the system runs.
#[cfg(target_os = "macos")]
#[repr(C, align(8))]
struct UniqueIdentifierInfo {
    _uuid: [u8; 16],
    unique_id: u64,
    parent_unique_id: u64,
    id_version: i32,
    _reserved: [u8; 20],
}

/// `PROC_PIDUNIQIDENTIFIERINFO`: a life's unique ids and pid version. The kernel answers it,
/// and `PROC_PIDT_SHORTBSDINFO`, for a process of any effective uid. It refuses
/// `PROC_PIDT_BSDINFOWITHUNIQID` with `EPERM` for a process whose effective uid is not the
/// caller's: a job member running a setuid-root program, such as the `/bin/ps` Nx runs to find a
/// task's processes when it is told to stop, whose real uid is still the job's. Measured on Darwin
/// 25.6 with `/usr/bin/top` (euid 0, ruid ours): flavor 18 answers `EPERM`, flavors 17 and 13
/// answer, and `proc_signal_with_audittoken` delivers STOP, CONT and TERM to it. An exited
/// process, reaped or not, answers `ESRCH` to all three.
#[cfg(target_os = "macos")]
const PROC_PIDUNIQIDENTIFIERINFO: libc::c_int = 17;

/// One running life as [`running_life`] reads it: its record, the pid version it carried at the
/// read's end, and the group and BSD flags it held within the read.
#[cfg(target_os = "macos")]
struct RunningLife {
    record: ProcessRecord,
    version: u32,
    pgid: u32,
    flags: u32,
}

/// `audit_token_t`.
#[cfg(target_os = "macos")]
#[repr(C)]
struct AuditToken {
    val: [u32; 8],
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    /// Signals the process the token's pid and pid version name, if it still runs. Returns
    /// the error number itself, not -1 (measured: `ESRCH` for a stale version, an exited and a
    /// reaped process; `EPERM` where the sandbox denies signalling; `EINVAL` for signal 0, so it
    /// cannot probe whether a process runs).
    fn proc_signal_with_audittoken(token: *mut AuditToken, signal: libc::c_int) -> libc::c_int;
}

/// Every running process that holds the group id, each with the identity it had when read.
#[cfg(target_os = "macos")]
fn members_of(pgid: i32) -> io::Result<Vec<Process>> {
    Ok(running_in_group(pgid)?
        .into_iter()
        .map(|life| Process {
            pid: life.record.pid.cast_signed(),
            version: life.version,
        })
        .collect())
}

/// The record of every running process that holds the group id.
#[cfg(target_os = "macos")]
fn running_in_group(pgid: i32) -> io::Result<Vec<RunningLife>> {
    let mut members = Vec::new();
    for pid in cowshed_os::process::group_pids(pgid)? {
        // Exited (an unreaped process answers ESRCH too) or gone: nothing to signal.
        let Some(life) = running_life(pid)? else {
            continue;
        };
        // The group was read inside the life's identity fence, so it is this very process's:
        // a newer process given the id would have answered with another unique id.
        if i32::try_from(life.pgid).ok() == Some(pgid) {
            members.push(life);
        }
    }
    Ok(members)
}

/// Signal the life `pid` holds while the kernel's unique id for it is `unique_id`; `false` once
/// that life no longer runs. Its pid version is read in the same call as that id, so a signal
/// reaches that life or nothing: a later process given the pid has another id. An exec gives the
/// life a new version and turns the signal away (`ESRCH`); the life is then read again by its
/// id, never renewed from the pid alone. A refusal under the version just read means the life
/// is exiting.
#[cfg(target_os = "macos")]
pub(crate) fn signal_life(pid: u32, unique_id: u64, signal: libc::c_int) -> io::Result<bool> {
    let pid = i32::try_from(pid).map_err(io::Error::other)?;
    let mut refused = None;
    loop {
        match signal_step(running_identity(pid), unique_id, refused)? {
            SignalStep::Gone => return Ok(false),
            SignalStep::Send(version) => {
                if (Process { pid, version }).signal(signal)? {
                    return Ok(true);
                }
                refused = Some(version);
            }
        }
    }
}

/// One step of signalling a life by its unique id, decided from the identity just read and the pid
/// version a signal was last refused under ([`signal_life`]).
#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SignalStep {
    /// Signal the life under this pid version.
    Send(u32),
    /// Stop: the life no longer runs (no life, or another one, holds the pid), or a signal was
    /// already refused under this very version, so the life is exiting.
    Gone,
}

/// [`SignalStep`] from `identity`, what flavor 17 answered for the pid. That flavor answers for a
/// process of any effective uid, a member running a setuid-root program included, so its read is
/// the retirement's one proof. A read that fails, `EPERM` included, is the step's error: a life
/// that cannot be read is never taken as gone, and its retirement says so instead of ending.
#[cfg(target_os = "macos")]
fn signal_step(
    identity: io::Result<Option<UniqueIdentifierInfo>>,
    unique_id: u64,
    refused: Option<u32>,
) -> io::Result<SignalStep> {
    let Some(identity) = identity?.filter(|identity| identity.unique_id == unique_id) else {
        return Ok(SignalStep::Gone);
    };
    let version = identity.id_version.cast_unsigned();
    Ok(if refused == Some(version) {
        SignalStep::Gone
    } else {
        SignalStep::Send(version)
    })
}

/// The life one pid holds, decided from its identity reads in their order: its unique ids
/// (`PROC_PIDUNIQIDENTIFIERINFO`), its short BSD info (`PROC_PIDT_SHORTBSDINFO`), its start time
/// (`KERN_PROC_PID`), and its unique ids again. These are the reads the kernel answers for a
/// process of any effective uid, so a member running a setuid-root program reads like any other;
/// `PROC_PIDT_BSDINFOWITHUNIQID` and `proc_pid_rusage`, which decline such a process with `EPERM`,
/// prove nothing here. The first read that finds no running life (`ESRCH`, `None`) answers
/// `None`; the first that fails otherwise, `EPERM` included, is the answer: an unreadable process
/// is never an absent one. Unique ids that changed between the first and the last read are
/// another life, so the pid holds none of this one.
#[cfg(target_os = "macos")]
fn decide_life(
    before: io::Result<Option<UniqueIdentifierInfo>>,
    bsd: io::Result<Option<libc::proc_bsdshortinfo>>,
    started: io::Result<Option<u64>>,
    after: io::Result<Option<UniqueIdentifierInfo>>,
) -> io::Result<Option<RunningLife>> {
    let Some(before) = before? else {
        return Ok(None);
    };
    let Some(bsd) = bsd? else {
        return Ok(None);
    };
    let Some(started) = started? else {
        return Ok(None);
    };
    let Some(after) = after? else {
        return Ok(None);
    };
    if before.unique_id != after.unique_id {
        return Ok(None);
    }
    Ok(Some(RunningLife {
        record: ProcessRecord {
            pid: bsd.pbsi_pid,
            ppid: bsd.pbsi_ppid,
            unique_id: after.unique_id,
            parent_unique_id: after.parent_unique_id,
            // xnu `proc_pidshortbsdinfo`: `pbsi_uid` is the credential's effective uid.
            uid: bsd.pbsi_uid,
            started_seconds: started / 1_000_000,
        },
        version: after.id_version.cast_unsigned(),
        pgid: bsd.pbsi_pgid,
        flags: bsd.pbsi_flags,
    }))
}

/// The identity of the life `pid` holds now; `None` once it has exited, reaped or not.
#[cfg(target_os = "macos")]
fn running_identity(pid: i32) -> io::Result<Option<UniqueIdentifierInfo>> {
    // SAFETY: flavor 17 writes exactly struct proc_uniqidentifierinfo.
    unsafe { pid_info(pid, PROC_PIDUNIQIDENTIFIERINFO) }
}

/// The public metadata of the life `pid` holds now, fenced by its immutable unique id; `None` once
/// it has exited, reaped or not. BSD credentials, ppid and group are a snapshot of that life, not
/// an atomic image claim or signal authority. An exec may change the pid version without changing
/// the birth or ancestry. No failed call is classified as absence.
#[cfg(target_os = "macos")]
fn running_life(pid: i32) -> io::Result<Option<RunningLife>> {
    let before = running_identity(pid);
    // SAFETY: SHORTBSDINFO writes exactly libc's struct proc_bsdshortinfo.
    let bsd = unsafe { pid_info::<libc::proc_bsdshortinfo>(pid, libc::PROC_PIDT_SHORTBSDINFO) };
    let started = start_time(pid);
    let after = running_identity(pid);
    decide_life(before, bsd, started, after)
}

/// Read one exact libproc payload; only ESRCH proves that no running life holds the pid.
///
/// # Safety
/// T is the C payload structure for flavor, with no invalid initialized bit patterns.
#[cfg(target_os = "macos")]
unsafe fn pid_info<T>(pid: i32, flavor: libc::c_int) -> io::Result<Option<T>> {
    let mut info = std::mem::MaybeUninit::<T>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<T>()).map_err(io::Error::other)?;
    // SAFETY: the caller supplies the exact payload type, writable for size bytes.
    let written = unsafe { libc::proc_pidinfo(pid, flavor, 0, info.as_mut_ptr().cast(), size) };
    if written != size {
        if written <= 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(None);
            }
            return Err(error);
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "proc_pidinfo flavor {flavor} for process {pid} returned {written} bytes, expected {size}"
            ),
        ));
    }
    // SAFETY: proc_pidinfo filled the exact payload, whose initialized bits the caller admits.
    Ok(Some(unsafe { info.assume_init() }))
}
/// A running process as the kernel names it: its pid, its parent's pid, the unique ids the
/// kernel never gives another process for both, its effective uid and its start second.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessRecord {
    pub pid: u32,
    pub ppid: u32,
    pub unique_id: u64,
    pub parent_unique_id: u64,
    pub uid: u32,
    pub started_seconds: u64,
}

/// The public metadata of the life `pid` holds now ([`running_life`]).
#[cfg(target_os = "macos")]
pub(crate) fn process_record(pid: i32) -> io::Result<Option<ProcessRecord>> {
    Ok(running_life(pid)?.map(|life| life.record))
}

/// The kernel's identity of the life `pid` names now: its unique id (`p_uniqueid`), which the
/// kernel never gives another process. `None` once it has exited, reaped or not.
#[cfg(target_os = "macos")]
pub(crate) fn life_of(pid: i32) -> io::Result<Option<u64>> {
    Ok(process_record(pid)?.map(|record| record.unique_id))
}

#[cfg(target_os = "macos")]
impl ProcessRecord {
    /// The life this record is of ([`Life`]).
    pub(crate) fn life(&self) -> Life {
        Life {
            pid: self.pid.cast_signed(),
            unique_id: self.unique_id,
        }
    }
}
#[cfg(target_os = "macos")]
impl Process {
    /// This process's record, read now; `None` once it has exited, or once its pid names a later
    /// process, whose record is never this one's: the pid's running process must still carry this
    /// process's version.
    pub(crate) fn record(&self) -> io::Result<Option<ProcessRecord>> {
        Ok(running_life(self.pid)?
            .filter(|life| life.version == self.version)
            .map(|life| life.record))
    }

    /// The process at the other end of a connected Unix socket, as the kernel recorded it when
    /// the connection was made: whatever happens to its pid later, the handle names only it.
    pub fn of_socket_peer(socket: &impl std::os::fd::AsRawFd) -> io::Result<Self> {
        let mut token = AuditToken { val: [0; 8] };
        let mut length = libc::socklen_t::try_from(std::mem::size_of::<AuditToken>())
            .map_err(io::Error::other)?;
        // SAFETY: `token` is writable storage of `length` bytes for the option's value.
        let read = unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::SOL_LOCAL,
                libc::LOCAL_PEERTOKEN,
                std::ptr::from_mut(&mut token).cast(),
                &mut length,
            )
        };
        if read != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            pid: token.val[5].cast_signed(),
            version: token.val[7],
        })
    }

    /// The process `pid` names now, held by the identity its kernel record carries; `None` once it
    /// has exited, reaped or not. A pid below one names no process, so it is refused, not read.
    pub fn observe(pid: i32) -> io::Result<Option<Self>> {
        if pid < 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("pid {pid} names no process"),
            ));
        }
        Ok(running_identity(pid)?.map(|identity| Self {
            pid,
            version: identity.id_version.cast_unsigned(),
        }))
    }

    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Whether this very life still runs: its pid's running process carries its pid version, which
    /// no later process given the pid has.
    pub(crate) fn running(&self) -> io::Result<bool> {
        Ok(self.record()?.is_some())
    }

    /// Signal the process; `false` when it no longer runs.
    pub fn signal(&self, signal: libc::c_int) -> io::Result<bool> {
        let mut token = AuditToken { val: [0; 8] };
        token.val[5] = self.pid.cast_unsigned();
        token.val[7] = self.version;
        // SAFETY: `token` is a live audit token the call only reads.
        match unsafe { proc_signal_with_audittoken(&mut token, signal) } {
            0 => Ok(true),
            libc::ESRCH => Ok(false),
            libc::EPERM => {
                let after = running_life(self.pid)
                    .map(|life| life.map(|life| (life.record.uid, life.flags, life.version)));
                let running = after
                    .as_ref()
                    .map(|life| life.map(|(_, _, version)| version));
                match permission_refusal(self.version, running) {
                    PermissionRefusal::Gone => Ok(false),
                    PermissionRefusal::Denied => Err(self.signal_permission_denied(signal, after)),
                }
            }
            error => Err(io::Error::new(
                io::Error::from_raw_os_error(error).kind(),
                format!(
                    "signaling process {} with signal {signal} failed: {}",
                    self.pid,
                    io::Error::from_raw_os_error(error)
                ),
            )),
        }
    }

    /// The denial, with the facts read after the refusal (`post_failure`) and the caller's uid.
    #[cold]
    fn signal_permission_denied(
        &self,
        signal: libc::c_int,
        post_failure: io::Result<Option<(u32, u32, u32)>>,
    ) -> io::Error {
        // SAFETY: geteuid takes no pointers and only reads the caller's credential.
        let caller_euid = unsafe { libc::geteuid() };
        let source = io::Error::from_raw_os_error(libc::EPERM);
        io::Error::new(
            source.kind(),
            SignalPermissionDenied {
                pid: self.pid,
                signal,
                expected_version: self.version,
                caller_euid,
                post_failure,
                source,
            },
        )
    }

    /// Watch for this process's exit, from now. A watch registered by pid once the process has
    /// begun to exit is refused (`ESRCH`) while its descriptors may still be open, and one made
    /// after it is gone could name another process: so it must be watched while it runs. The
    /// pid's running process read after the registration carries this process's pid version,
    /// which no other process is given: it ran throughout, and the watch names it.
    pub fn watch_exit(&self) -> io::Result<ExitWatch> {
        let watch = ExitWatch::register(self.pid).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "process {} cannot be watched to its exit, which may have begun: {error}",
                    self.pid
                ),
            )
        })?;
        match running_identity(self.pid)? {
            Some(identity) if identity.id_version.cast_unsigned() == self.version => Ok(watch),
            Some(_) => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "process {} exited before it could be watched; its pid names another",
                    self.pid
                ),
            )),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("process {} exited before it could be watched", self.pid),
            )),
        }
    }

    /// The bytes this process holds resident now; `None` once it has exited. The read names
    /// only the pid, so it counts only if the pid's running process still carries this
    /// process's version afterwards: then this process ran throughout and the size read was its
    /// own, not that of a later process given the pid. A read that failed for a process gone
    /// since is its exit, not an error.
    pub(super) fn resident_bytes(&self) -> io::Result<Option<u64>> {
        let read = cowshed_os::process::resident_bytes(self.pid);
        match running_identity(self.pid)? {
            Some(identity) if identity.id_version.cast_unsigned() == self.version => read,
            Some(_) | None => Ok(None),
        }
    }

    /// The ports of the TCP sockets this process listens on now; `None` once it has exited. The
    /// read names only the pid, so this process's identity, read afterwards, judges it: still
    /// carrying this process's version, it ran throughout and the read -- its answer or why it
    /// failed -- was its own; gone, or another process's now, it holds nothing, whatever the
    /// read met.
    pub(super) fn listening_ports(&self) -> io::Result<Option<BTreeSet<u16>>> {
        let read = cowshed_os::process::listening_ports(self.pid);
        match running_identity(self.pid)? {
            Some(identity) if identity.id_version.cast_unsigned() == self.version => read.map(Some),
            Some(_) | None => Ok(None),
        }
    }
}

/// One process through every image it runs: its pid and the unique id the kernel never gives
/// another process (`p_uniqueid`). An exec gives the pid a new version, which ends a [`Process`]
/// held from before it, but keeps the unique id, so a life is found again after one.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Life {
    pub(crate) pid: i32,
    pub(crate) unique_id: u64,
}

#[cfg(target_os = "macos")]
impl Life {
    /// The process this life is now, in one read of its pid; `None` once it has exited, reaped
    /// or not, since no other process is given its unique id.
    pub(crate) fn current(self) -> io::Result<Option<Process>> {
        Ok(running_identity(self.pid)?
            .filter(|identity| identity.unique_id == self.unique_id)
            .map(|identity| Process {
                pid: self.pid,
                version: identity.id_version.cast_unsigned(),
            }))
    }

    /// The life `pid` names now; `None` once it has exited, reaped or not.
    pub fn of(pid: i32) -> io::Result<Option<Self>> {
        Ok(running_identity(pid)?.map(|identity| Self {
            pid,
            unique_id: identity.unique_id,
        }))
    }

    /// Its exit, watched from now through any exec; `Before` once it has exited: a later process
    /// given its pid is never taken for it ([`Process::watch_life`]).
    pub fn exit(&self) -> io::Result<ProcessExit> {
        Ok(self
            .watch_exit()?
            .map_or(ProcessExit::Before, ProcessExit::Watching))
    }

    /// Watch this life's exit from now, through any exec; `None` once it has exited. The pid's
    /// running process read after the registration still carries this life's unique id: it ran
    /// throughout, and the watch names it.
    pub(crate) fn watch_exit(self) -> io::Result<Option<ExitWatch>> {
        let watch = match ExitWatch::register(self.pid) {
            Ok(watch) => watch,
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(self.current()?.map(|_| watch))
    }
}
/// A process the trusted ledger proves is a job's own: a member of a recorded group whose
/// ownership held throughout ([`ownership`]). Only [`owned_process`] makes one, so a caller holding
/// it has the proof and no other process can enter the privileged signal path through it.
#[cfg(target_os = "macos")]
pub(crate) struct OwnedGroupProcess(Process);

#[cfg(target_os = "macos")]
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
#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
impl Process {
    /// Whether `other` is this very life: the same pid at the same pid version, which no later
    /// process given the pid carries.
    pub(crate) fn same_life(&self, other: &Self) -> io::Result<bool> {
        Ok(self.pid == other.pid && self.version == other.version)
    }

    /// Watch for this process's exit from now; `None` once it has exited or begun to: the kernel
    /// refuses a new watch of an exiting process (`ESRCH`), and a pid whose running process no
    /// longer carries this version names another.
    fn watch_running(&self) -> io::Result<Option<ExitWatch>> {
        let watch = match ExitWatch::register(self.pid) {
            Ok(watch) => watch,
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(self.running()?.then_some(watch))
    }
}

#[cfg(target_os = "linux")]
impl Process {
    /// Whether `other` is this very life. Each names a process that held the pid when its
    /// connection was made, and while this one has not exited the pid names no other process.
    pub(crate) fn same_life(&self, other: &Self) -> io::Result<bool> {
        Ok(self.pid == other.pid && !pidfd_exited(&self.handle)?)
    }

    /// Watch for this process's exit: its pidfd names it alone, exited or not.
    fn watch_running(&self) -> io::Result<Option<ExitWatch>> {
        self.watch_exit().map(Some)
    }
}

/// A process followed by its pidfd, which names it across every exec ([`Process::watch_life`]).
#[cfg(target_os = "linux")]
pub struct Life(Process);

#[cfg(target_os = "linux")]
impl Life {
    /// The life `pid` names now; `None` once it has exited.
    pub fn of(pid: i32) -> io::Result<Option<Self>> {
        Ok(Process::observe(pid)?.map(Self))
    }

    /// Its exit, watched from now; `Before` once it has exited.
    pub fn exit(&self) -> io::Result<ProcessExit> {
        self.0.exit()
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

/// A process held by an identity the kernel never gives another: a pidfd, which names the one
/// process it was opened for. Signalling it reaches that process or nothing.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct Process {
    pid: i32,
    handle: std::os::fd::OwnedFd,
}

/// Every running process that holds the group id, each held by a pidfd.
#[cfg(target_os = "linux")]
fn members_of(pgid: i32) -> io::Result<Vec<Process>> {
    use std::os::fd::FromRawFd as _;

    let mut members = Vec::new();
    for entry in std::fs::read_dir("/proc")? {
        let Some(pid) = entry?
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
        else {
            continue;
        };
        // SAFETY: pidfd_open takes plain integers; the descriptor is owned from here on.
        let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if opened < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                continue;
            }
            return Err(error);
        }
        let opened = libc::c_int::try_from(opened).map_err(io::Error::other)?;
        // SAFETY: a fresh descriptor this function alone owns.
        let handle = unsafe { std::os::fd::OwnedFd::from_raw_fd(opened) };
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"));
        let Some(stat) = stat_while_running(&handle, stat)? else {
            continue;
        };
        let closing = stat.rfind(')').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "process stat has no command delimiter",
            )
        })?;
        let mut fields = stat[closing + 1..].split_whitespace();
        let state = fields.next();
        let group = fields.nth(1).and_then(|group| group.parse::<i32>().ok());
        if state != Some("Z") && group == Some(pgid) {
            members.push(Process { pid, handle });
        }
    }
    Ok(members)
}

/// Resolve a proc-stat read against the lifetime of the process its pidfd names.
#[cfg(target_os = "linux")]
fn stat_while_running(
    handle: &std::os::fd::OwnedFd,
    read: io::Result<String>,
) -> io::Result<Option<String>> {
    // A departed process is no current member, even when its proc-stat read returned ESRCH.
    // Only a process still live afterwards can contribute its stat or its read failure.
    if pidfd_exited(handle)? {
        return Ok(None);
    }
    read.map(Some)
}

#[cfg(target_os = "linux")]
impl Process {
    /// The process `pid` names now, held by a pidfd, which names that one process whatever later
    /// takes its pid; `None` once it has exited. A pid below one names no process, so it is
    /// refused, not read.
    pub fn observe(pid: i32) -> io::Result<Option<Self>> {
        use std::os::fd::FromRawFd as _;

        if pid < 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("pid {pid} names no process"),
            ));
        }
        // SAFETY: pidfd_open takes plain integers; the descriptor is owned from here on.
        let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if opened < 0 {
            let error = io::Error::last_os_error();
            return match error.raw_os_error() {
                Some(libc::ESRCH) => Ok(None),
                _ => Err(error),
            };
        }
        let opened = libc::c_int::try_from(opened).map_err(io::Error::other)?;
        // SAFETY: a fresh descriptor this function alone owns.
        let handle = unsafe { std::os::fd::OwnedFd::from_raw_fd(opened) };
        if pidfd_exited(&handle)? {
            return Ok(None);
        }
        Ok(Some(Self { pid, handle }))
    }

    /// The process at the other end of a connected Unix socket, as the kernel recorded it when
    /// the connection was made: whatever happens to its pid later, the handle names only it.
    pub fn of_socket_peer(socket: &impl std::os::fd::AsRawFd) -> io::Result<Self> {
        use std::os::fd::FromRawFd as _;

        let mut opened: libc::c_int = -1;
        let mut length = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
            .map_err(io::Error::other)?;
        // SAFETY: `opened` is writable storage of `length` bytes for the option's value.
        if unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERPIDFD,
                std::ptr::from_mut(&mut opened).cast(),
                &mut length,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the kernel handed this process a fresh descriptor it alone owns.
        let handle = unsafe { std::os::fd::OwnedFd::from_raw_fd(opened) };
        let mut credentials = libc::ucred {
            pid: 0,
            uid: 0,
            gid: 0,
        };
        let mut length = libc::socklen_t::try_from(std::mem::size_of::<libc::ucred>())
            .map_err(io::Error::other)?;
        // SAFETY: `credentials` is writable storage of `length` bytes for the option's value.
        if unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                std::ptr::from_mut(&mut credentials).cast(),
                &mut length,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            pid: credentials.pid,
            handle,
        })
    }

    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Whether this very life still runs: its pidfd names it alone, whatever later takes its pid.
    pub(crate) fn running(&self) -> io::Result<bool> {
        Ok(!pidfd_exited(&self.handle)?)
    }

    /// Signal the process; `false` when it no longer runs.
    pub fn signal(&self, signal: libc::c_int) -> io::Result<bool> {
        use std::os::fd::AsRawFd as _;

        // SAFETY: pidfd_send_signal takes a live descriptor and plain integers.
        let sent = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.handle.as_raw_fd(),
                signal,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        if sent == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(false);
        }
        Err(error)
    }

    /// Watch for this process's exit: its pidfd names it alone, whenever it is polled.
    pub fn watch_exit(&self) -> io::Result<ExitWatch> {
        self.handle.try_clone().map(ExitWatch::from_pidfd)
    }

    /// The bytes this process holds resident now; `None` once it has exited. The read names
    /// only the pid, so it counts only if the pidfd -- which names this process alone -- says it
    /// still runs afterwards: then the size read was its own, not that of a later process given
    /// the pid. A read that failed for a process gone since is its exit, not an error.
    pub(super) fn resident_bytes(&self) -> io::Result<Option<u64>> {
        let read = cowshed_os::process::resident_bytes(self.pid);
        if pidfd_exited(&self.handle)? {
            return Ok(None);
        }
        read
    }

    /// The ports of the TCP sockets this process listens on now; `None` once it has exited. The
    /// pidfd, polled after the read, judges it: still running, this process's descriptors and
    /// network tables were read -- or failed to be -- as its own; exited, it holds nothing,
    /// whatever the read met. A missing table of a process still running is an error.
    pub(super) fn listening_ports(&self) -> io::Result<Option<BTreeSet<u16>>> {
        let read = cowshed_os::process::listening_ports(self.pid);
        if pidfd_exited(&self.handle)? {
            return Ok(None);
        }
        read.map(Some)
    }
}

/// Whether the process `handle` names has exited: a pidfd becomes readable when it does.
#[cfg(target_os = "linux")]
fn pidfd_exited(handle: &std::os::fd::OwnedFd) -> io::Result<bool> {
    use std::os::fd::AsRawFd as _;

    let mut exited = libc::pollfd {
        fd: handle.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one live pollfd entry.
    if unsafe { libc::poll(&mut exited, 1, 0) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(exited.revents != 0)
}

/// `struct kinfo_proc` (`<sys/sysctl.h>`), which the `libc` crate does not declare. Only its
/// leading field is read: `kp_proc.p_un.__p_starttime`, the `p_start` the kernel stamps at fork
/// and `proc_pidinfo` reports as `pbi_start`.
#[cfg(target_os = "macos")]
#[repr(C)]
struct KinfoProc {
    start: libc::timeval,
    _rest: [u8; 632],
}

#[cfg(target_os = "macos")]
const _: () = assert!(std::mem::size_of::<KinfoProc>() == 648);

/// A process's start time: the calendar time its fork stamped, in microseconds, or confirmed
/// absence. `KERN_PROC_PID` answers it for every process whoever owns it -- unlike
/// `proc_pidinfo` and `proc_pid_rusage`, which decline another user's process with `EPERM` --
/// and for one that exited and is not yet reaped; `None` once nothing holds the pid. An
/// unreadable process is never absent.
#[cfg(target_os = "macos")]
fn start_time(pid: i32) -> io::Result<Option<u64>> {
    let mut name = [libc::CTL_KERN, libc::KERN_PROC, libc::KERN_PROC_PID, pid];
    let mut info = std::mem::MaybeUninit::<KinfoProc>::zeroed();
    let mut size = std::mem::size_of::<KinfoProc>();
    // SAFETY: `name` holds the four MIB entries passed, and `info` is writable storage of
    // `size` bytes.
    let read = unsafe {
        libc::sysctl(
            name.as_mut_ptr(),
            4,
            info.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if read != 0 {
        return Err(io::Error::last_os_error());
    }
    if size == 0 {
        return Ok(None);
    }
    if size != std::mem::size_of::<KinfoProc>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "KERN_PROC_PID returned {size} bytes, expected {}",
                std::mem::size_of::<KinfoProc>()
            ),
        ));
    }
    // SAFETY: sysctl filled all `size` bytes.
    let start = unsafe { info.assume_init() }.start;
    let invalid = |error| io::Error::new(io::ErrorKind::InvalidData, error);
    let seconds = u64::try_from(start.tv_sec).map_err(invalid)?;
    let micros = u64::try_from(start.tv_usec).map_err(invalid)?;
    seconds
        .checked_mul(1_000_000)
        .and_then(|seconds| seconds.checked_add(micros))
        .map(Some)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "process start time overflowed"))
}

/// A process's birth time as `proc_pid_rusage` reports it, which -- unlike `proc_pidinfo` --
/// still answers for a process that exited and is not yet reaped. `None` once it is reaped.
#[cfg(target_os = "macos")]
fn birth_time(pid: i32) -> io::Result<Option<u64>> {
    let mut info = std::mem::MaybeUninit::<libc::rusage_info_v0>::zeroed();
    // SAFETY: `info` is writable storage of the size the V0 flavor writes.
    let result =
        unsafe { libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V0, info.as_mut_ptr().cast()) };
    if result != 0 {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(None)
        } else {
            Err(error)
        };
    }
    // SAFETY: a successful call filled the V0 record.
    Ok(Some(unsafe { info.assume_init() }.ri_proc_start_abstime))
}

/// A process's birth time: procfs keeps an exited process's stat until it is reaped.
#[cfg(target_os = "linux")]
fn birth_time(pid: i32) -> io::Result<Option<u64>> {
    start_time(pid)
}

/// The leader's start time in ticks since boot, or confirmed absence on a mounted procfs.
#[cfg(target_os = "linux")]
fn start_time(pid: i32) -> io::Result<Option<u64>> {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            std::fs::metadata("/proc/self/stat")?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    // The command name is parenthesized and may hold spaces; fields resume after the last ')'.
    let closing = stat.rfind(')').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "process stat has no command delimiter",
        )
    })?;
    let start = stat[closing + 1..]
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "process stat has no start time")
        })?;
    start
        .parse()
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// The identity of the life `pid` names now: its start time in clock ticks since boot, which no
/// later process given the pid shares. `None` once nothing holds the pid.
#[cfg(target_os = "linux")]
pub(crate) fn life_of(pid: i32) -> io::Result<Option<u64>> {
    start_time(pid)
}

#[cfg(test)]
mod tests {
    use cowshed_os::test_backstop::ChildBackstop as _;
    #[cfg(target_os = "macos")]
    use cowshed_os::test_backstop::CommandBackstop as _;
    use std::collections::BTreeSet;
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    use super::{GroupLeader, Writer, end_recorded, ledger_path, names_groups, take_lost};
    use cowshed_os::fork_lock::Spawn as _;

    fn leader(pid: u32) -> GroupLeader {
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
    fn job_group() -> std::process::Child {
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
    struct OwnedGroup(std::process::Child);

    impl Drop for OwnedGroup {
        fn drop(&mut self) {
            let pgid = i32::try_from(self.0.id()).expect("owned child pid");
            // SAFETY: this unreaped child is the group leader spawned by this test.
            unsafe { libc::killpg(pgid, libc::SIGKILL) };
            let _ = self.0.wait_bounded();
        }
    }

    #[cfg(target_os = "macos")]
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

    /// A life is signalled only under its own kernel unique id, read with its pid version in one
    /// call: the same pid under another id is never reached, and a life that exited -- held
    /// unreaped, so its pid names nothing else -- is reached by nothing.
    #[cfg(target_os = "macos")]
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

    /// What `PROC_PIDUNIQIDENTIFIERINFO` answers for a life: its unique id and pid version.
    #[cfg(target_os = "macos")]
    fn identity(unique_id: u64, version: i32) -> super::UniqueIdentifierInfo {
        super::UniqueIdentifierInfo {
            _uuid: [0; 16],
            unique_id,
            parent_unique_id: 1,
            id_version: version,
            _reserved: [0; 20],
        }
    }

    /// What `PROC_PIDT_SHORTBSDINFO` answers for a group leader of effective uid `uid`.
    #[cfg(target_os = "macos")]
    fn short_bsd(uid: u32) -> libc::proc_bsdshortinfo {
        // SAFETY: proc_bsdshortinfo is plain C data; all-zero is a valid value.
        let mut bsd: libc::proc_bsdshortinfo = unsafe { std::mem::zeroed() };
        bsd.pbsi_pid = 42;
        bsd.pbsi_ppid = 1;
        bsd.pbsi_pgid = 42;
        bsd.pbsi_uid = uid;
        bsd
    }

    #[cfg(target_os = "macos")]
    fn errno<T>(code: libc::c_int) -> std::io::Result<T> {
        Err(std::io::Error::from_raw_os_error(code))
    }

    /// Which reads prove a member's life, and what an `EPERM` from one means: a process of root's
    /// effective uid (a member running a setuid-root program) reads like any other from the
    /// flavors that answer every uid; the first read that finds no life answers absent; the first
    /// that fails otherwise, `EPERM` included, is unreadable, never absent; a unique id that
    /// changed between the fence's two reads is another life.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_life_is_decided_from_the_reads_every_effective_uid_answers() {
        let life = super::decide_life(
            Ok(Some(identity(7, 3))),
            Ok(Some(short_bsd(0))),
            Ok(Some(5_000_000)),
            Ok(Some(identity(7, 3))),
        )
        .expect("every read answered")
        .expect("one life held the pid throughout");
        assert_eq!(
            (
                life.record.uid,
                life.record.unique_id,
                life.version,
                life.pgid,
                life.record.started_seconds
            ),
            (0, 7, 3, 42, 5)
        );
        type Reads = (
            std::io::Result<Option<super::UniqueIdentifierInfo>>,
            std::io::Result<Option<libc::proc_bsdshortinfo>>,
            std::io::Result<Option<u64>>,
            std::io::Result<Option<super::UniqueIdentifierInfo>>,
        );
        type Case = (&'static str, fn() -> Reads, Result<bool, libc::c_int>);
        let cases: [Case; 7] = [
            (
                "gone at the first read",
                || (Ok(None), Ok(Some(short_bsd(0))), Ok(Some(1)), Ok(None)),
                Ok(false),
            ),
            (
                "gone first, declined after: the first answer stands",
                || (Ok(None), errno(libc::EPERM), Ok(Some(1)), Ok(None)),
                Ok(false),
            ),
            (
                "its unique ids declined",
                || {
                    (
                        errno(libc::EPERM),
                        Ok(Some(short_bsd(0))),
                        Ok(Some(1)),
                        Ok(Some(identity(7, 3))),
                    )
                },
                Err(libc::EPERM),
            ),
            (
                "its BSD info declined",
                || {
                    (
                        Ok(Some(identity(7, 3))),
                        errno(libc::EPERM),
                        Ok(Some(1)),
                        Ok(Some(identity(7, 3))),
                    )
                },
                Err(libc::EPERM),
            ),
            (
                "its start time declined",
                || {
                    (
                        Ok(Some(identity(7, 3))),
                        Ok(Some(short_bsd(0))),
                        errno(libc::EPERM),
                        Ok(Some(identity(7, 3))),
                    )
                },
                Err(libc::EPERM),
            ),
            (
                "exited before the fence closed",
                || {
                    (
                        Ok(Some(identity(7, 3))),
                        Ok(Some(short_bsd(0))),
                        Ok(Some(1)),
                        Ok(None),
                    )
                },
                Ok(false),
            ),
            (
                "another life by the fence's end",
                || {
                    (
                        Ok(Some(identity(7, 3))),
                        Ok(Some(short_bsd(0))),
                        Ok(Some(1)),
                        Ok(Some(identity(8, 1))),
                    )
                },
                Ok(false),
            ),
        ];
        for (case, reads, expected) in cases {
            let (before, bsd, started, after) = reads();
            let decided = super::decide_life(before, bsd, started, after);
            match (decided, expected) {
                (Ok(life), Ok(present)) => assert_eq!(life.is_some(), present, "{case}"),
                (Err(error), Err(code)) => assert_eq!(error.raw_os_error(), Some(code), "{case}"),
                (Ok(life), Err(code)) => panic!(
                    "{case}: decided {:?}, expected errno {code}",
                    life.map(|life| life.record)
                ),
                (Err(error), Ok(_)) => panic!("{case}: decided {error}"),
            }
        }
    }

    /// What a signal refused with `EPERM` means, from the life read after the refusal: the
    /// kernel refuses an audit token whose process has exited, or whose pid another life now holds,
    /// with `EPERM` as well, so only the very life still running is a denial; a read that fails
    /// proves nothing gone, so it stays a denial.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_refused_signal_is_a_denial_only_while_the_signalled_life_runs() {
        use super::PermissionRefusal::{Denied, Gone};
        let unreadable = std::io::Error::from_raw_os_error(libc::EPERM);
        type Case<'a> = (
            &'static str,
            Result<Option<u32>, &'a std::io::Error>,
            super::PermissionRefusal,
        );
        let cases: [Case<'_>; 4] = [
            ("the signalled life runs", Ok(Some(88)), Denied),
            ("no life holds the pid", Ok(None), Gone),
            ("another life holds the pid", Ok(Some(89)), Gone),
            ("the pid cannot be read", Err(&unreadable), Denied),
        ];
        for (case, running, expected) in cases {
            assert_eq!(super::permission_refusal(88, running), expected, "{case}");
        }
    }

    /// What retirement does with the identity it reads before each signal: signal the life under
    /// the version just read; stop once no life or another holds the pid, or a signal was refused
    /// under this very version (the life is exiting); and an `EPERM` read is the retirement's
    /// error, never a life taken as gone.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_retirement_signals_the_life_it_reads_and_never_takes_an_unreadable_one_as_gone() {
        use super::SignalStep::{Gone, Send};
        type Identity = std::io::Result<Option<super::UniqueIdentifierInfo>>;
        type Case = (
            &'static str,
            fn() -> Identity,
            Option<u32>,
            Result<super::SignalStep, libc::c_int>,
        );
        let cases: [Case; 6] = [
            (
                "the life runs",
                || Ok(Some(identity(7, 3))),
                None,
                Ok(Send(3)),
            ),
            (
                "refused under this version: exiting",
                || Ok(Some(identity(7, 3))),
                Some(3),
                Ok(Gone),
            ),
            (
                "an exec gave it a new version",
                || Ok(Some(identity(7, 4))),
                Some(3),
                Ok(Send(4)),
            ),
            (
                "another life holds the pid",
                || Ok(Some(identity(8, 1))),
                None,
                Ok(Gone),
            ),
            ("no life holds the pid", || Ok(None), None, Ok(Gone)),
            (
                "its identity declined",
                || errno(libc::EPERM),
                None,
                Err(libc::EPERM),
            ),
        ];
        for (case, identity, refused, expected) in cases {
            match (super::signal_step(identity(), 7, refused), expected) {
                (Ok(step), Ok(expected)) => assert_eq!(step, expected, "{case}"),
                (Err(error), Err(code)) => assert_eq!(error.raw_os_error(), Some(code), "{case}"),
                (step, expected) => panic!("{case}: {step:?}, expected {expected:?}"),
            }
        }
    }

    /// A kqueue watch of one pid's exec (`EVFILT_PROC` `NOTE_EXEC`), registered before the exec.
    #[cfg(target_os = "macos")]
    struct ExecWatch(std::os::fd::OwnedFd);

    #[cfg(target_os = "macos")]
    impl ExecWatch {
        fn on(pid: u32) -> Self {
            use std::os::fd::{AsRawFd as _, FromRawFd as _};
            // SAFETY: kqueue takes nothing and returns a new descriptor or -1.
            let queue = unsafe { libc::kqueue() };
            assert!(queue >= 0, "kqueue: {}", std::io::Error::last_os_error());
            // SAFETY: `queue` is a new descriptor this watch owns.
            let queue = unsafe { std::os::fd::OwnedFd::from_raw_fd(queue) };
            let change = libc::kevent {
                ident: usize::try_from(pid).expect("a pid"),
                filter: libc::EVFILT_PROC,
                flags: libc::EV_ADD | libc::EV_ONESHOT,
                fflags: libc::NOTE_EXEC,
                data: 0,
                udata: std::ptr::null_mut(),
            };
            // SAFETY: one change record is read, no event buffer is written.
            let added = unsafe {
                libc::kevent(
                    queue.as_raw_fd(),
                    &raw const change,
                    1,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            };
            assert_eq!(
                added,
                0,
                "watch the exec of {pid}: {}",
                std::io::Error::last_os_error()
            );
            Self(queue)
        }

        /// Whether the exec came within `bound`.
        fn within(&self, bound: Duration) -> bool {
            use std::os::fd::AsRawFd as _;
            let timeout = libc::timespec {
                tv_sec: libc::time_t::try_from(bound.as_secs()).expect("a bound in seconds"),
                tv_nsec: libc::c_long::from(bound.subsec_nanos()),
            };
            loop {
                // SAFETY: an all-zero kevent is valid storage for the kernel to fill.
                let mut event: libc::kevent = unsafe { std::mem::zeroed() };
                // SAFETY: no change is read; one event record is written.
                let seen = unsafe {
                    libc::kevent(
                        self.0.as_raw_fd(),
                        std::ptr::null(),
                        0,
                        &raw mut event,
                        1,
                        &raw const timeout,
                    )
                };
                match seen {
                    1 => return event.fflags & libc::NOTE_EXEC != 0,
                    0 => return false,
                    _ if std::io::Error::last_os_error().kind()
                        == std::io::ErrorKind::Interrupted => {}
                    _ => panic!("wait for the exec: {}", std::io::Error::last_os_error()),
                }
            }
        }
    }

    /// The shell of the decisions above, on a real setuid-root program: a leader that execs
    /// `/usr/bin/top` (setuid root on every macOS host) runs as root's effective uid and the
    /// job's real uid. Read once its exec is seen (no uid poll), it reads with the flavors
    /// `decide_life` takes, while `proc_pid_rusage` declines it with `EPERM`; it proves its group
    /// running and once exited and held unreaped; and retirement's signal reaches it. The shell's
    /// sibling report proves its body started before the watch: /bin/sh first execs its selected
    /// shell, and that earlier NOTE_EXEC is not top's exec.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_a_setuid_root_leader_reads_proves_its_group_and_retires() {
        let (job, sibling) =
            group_with_child("sleep 600 & echo $!; read _; exec /usr/bin/top -l 0 -s 1 >/dev/null");
        let mut group = OwnedGroup(job);
        let pid = group.0.id();
        let leader_pid = i32::try_from(pid).expect("a pid");
        let birth = super::Birth::Observed(leader(pid));
        let exec = ExecWatch::on(pid);
        std::io::Write::write_all(group.0.stdin.as_mut().expect("piped stdin"), b"\n")
            .expect("let the leader exec top");
        assert!(
            exec.within(cowshed_os::test_backstop::NO_PROGRESS),
            "the leader did not exec /usr/bin/top within the test backstop"
        );
        let record = super::process_record(leader_pid)
            .expect("read the leader")
            .expect("top runs");
        assert_eq!(
            record.uid, 0,
            "precondition not met: /usr/bin/top runs as effective uid {}, not root. This check needs \
             setuid honored, outside every cow sandbox (nx run cowshed:host-controller-test)",
            record.uid
        );
        assert_eq!(
            super::birth_time(leader_pid)
                .expect_err("rusage declines another effective uid")
                .raw_os_error(),
            Some(libc::EPERM)
        );
        let members = super::job_member_records(&birth).expect("the job's membership");
        assert_eq!(
            members.len(),
            2,
            "the leader and its sleeping sibling: {members:?}"
        );
        assert!(
            members
                .iter()
                .any(|member| member.pid == pid && member.uid == 0),
            "{members:?}"
        );
        assert!(super::signal_life(pid, record.unique_id, libc::SIGTERM).expect("signal top"));
        assert_eq!(
            super::await_exit_unreaped(leader_pid).expect("top exits"),
            crate::api::dto::ExitStatus::Signaled {
                signal: libc::SIGTERM,
                core_dumped: false,
            }
        );
        assert_eq!(
            super::job_member_records(&birth)
                .expect("the exited leader's group")
                .iter()
                .map(|member| member.pid)
                .collect::<Vec<_>>(),
            [sibling]
        );
    }

    /// A group led by `sh` whose child is `script`'s last background job: the leader, held
    /// unreaped by the test, and the child's pid, which `script` prints. The leader's stdin is a
    /// pipe the returned leader holds open, for a child that waits on it.
    fn group_with_child(script: &str) -> (std::process::Child, u32) {
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

    #[cfg(target_os = "linux")]
    fn pipe_held_process() -> (std::process::Child, std::os::fd::OwnedFd) {
        use std::os::fd::FromRawFd as _;

        let child = Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .process_group(0)
            .spawn_locked()
            .expect("a pipe-held child");
        // SAFETY: pidfd_open takes plain integers; the child remains live on its stdin.
        let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, child.id(), 0) };
        assert!(
            opened >= 0,
            "pidfd_open: {}",
            std::io::Error::last_os_error()
        );
        let opened = libc::c_int::try_from(opened).expect("a pidfd fits a descriptor");
        // SAFETY: this fresh descriptor is owned only by this test.
        let handle = unsafe { std::os::fd::OwnedFd::from_raw_fd(opened) };
        (child, handle)
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_proc_stat_read_of_a_reaped_process_is_its_departure_not_a_membership_failure() {
        use std::io::Read as _;

        let (mut child, handle) = pipe_held_process();
        let mut stat = std::fs::File::open(format!("/proc/{}/stat", child.id()))
            .expect("open the live child's proc-stat inode");
        drop(child.stdin.take());
        assert!(
            child
                .wait_bounded()
                .expect("reap the departed child")
                .success()
        );
        let mut text = String::new();
        let read = stat.read_to_string(&mut text).map(|_| text);
        assert_eq!(
            read.as_ref().unwrap_err().raw_os_error(),
            Some(libc::ESRCH),
            "the open proc-stat inode reports its process was reaped"
        );
        assert_eq!(
            super::stat_while_running(&handle, read).unwrap(),
            None,
            "the retained pidfd proves this process departed during the read"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_proc_stat_read_failure_of_a_live_process_remains_an_error() {
        let (mut child, handle) = pipe_held_process();
        let failures = [libc::EACCES, libc::ESRCH, libc::ENOENT].map(|code| {
            (
                code,
                super::stat_while_running(&handle, Err(std::io::Error::from_raw_os_error(code))),
            )
        });
        drop(child.stdin.take());
        assert!(
            child
                .wait_bounded()
                .expect("reap the child after its live reads")
                .success()
        );
        for (code, read) in failures {
            assert_eq!(read.unwrap_err().raw_os_error(), Some(code));
        }
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

    #[cfg(target_os = "macos")]
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

    fn group_alive(pgid: i32) -> bool {
        // SAFETY: signal 0 only checks that the group exists.
        unsafe { libc::killpg(pgid, 0) == 0 }
    }

    /// A scratch ledger and the directory holding it. The ledger's own removal is under test; the
    /// directory goes when the test ends, on any path.
    fn scratch() -> (crate::temp_root::TempRoot, std::path::PathBuf) {
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

    /// A job-shaped process tree whose leader has finished starting: an exec gives a process a
    /// new pid version, and macOS's `/bin/sh` execs the shell it selects (measured: about 2ms
    /// after the spawn returns). Its leader announces itself only after that last exec, so the
    /// identity read once this returns is the one it keeps.
    #[cfg(target_os = "macos")]
    fn started_job_group() -> std::process::Child {
        use std::io::BufRead as _;

        let mut child = Command::new("/bin/sh")
            .args(["-c", "exec 3<&0; read _ <&3 & echo READY; wait"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("spawn a job group");
        let mut ready = String::new();
        std::io::BufReader::new(child.stdout.take().expect("its stdout"))
            .read_line(&mut ready)
            .expect("the leader announces itself");
        assert_eq!(ready.trim(), "READY");
        child
    }

    /// End what remains of a [`started_job_group`] once its leader is reaped: the group's id
    /// still names its backgrounded reader. Returns once no process holds the id, so nothing the
    /// test started outlives it holding the runner's output.
    #[cfg(target_os = "macos")]
    fn end_started_group(pgid: i32) {
        // SAFETY: the test's own group; its backgrounded reader may still hold the id.
        unsafe { libc::killpg(pgid, libc::SIGKILL) };
        while super::group_has_live_members(pgid).unwrap() {
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// A process is signalled only through the identity it was read with: the pid and that pid's
    /// version. The same pid under any other version -- the shape a reused pid takes -- is never
    /// reached, and neither is the process once it has exited and been reaped.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_signal_reaches_only_the_process_its_identity_names() {
        let mut job = started_job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let members = super::members_of(pgid).unwrap();
        let leader = members
            .iter()
            .find(|member| member.pid() == pgid)
            .expect("the running leader holds its group id");
        let other_incarnation = super::Process {
            pid: leader.pid,
            version: leader.version.wrapping_add(1),
        };
        assert!(!other_incarnation.signal(libc::SIGKILL).unwrap());
        assert!(
            group_alive(pgid),
            "a process under another version was signalled"
        );
        assert!(leader.signal(libc::SIGKILL).unwrap());
        job.wait_bounded().unwrap();
        assert!(
            !leader.signal(libc::SIGKILL).unwrap(),
            "a reaped process was signalled"
        );
        end_started_group(pgid);
    }

    /// A process that execs takes a new pid version, so a TERM sent under the identity read just
    /// before the exec reaches nothing while the process still runs as the job's. Its group is
    /// ended all the same: the KILL that follows the grace reads the group again. Here the group
    /// is read, its leader then execs, and the membership read before the exec is what TERM goes
    /// to -- the window a loaded host opens between a recovery's read and its TERM.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_group_whose_member_execed_after_it_was_read_is_still_ended() {
        use std::io::{BufRead as _, Write as _};

        let child = Command::new("/bin/sh")
            .args(["-c", "echo READY; read line; exec /bin/cat"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("spawn a job group");
        let mut job = OwnedGroup(child);
        let pgid = i32::try_from(job.0.id()).unwrap();
        let mut ready = String::new();
        std::io::BufReader::new(job.0.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        assert_eq!(ready.trim(), "READY");
        let group = super::Group {
            job_id: 6,
            pgid,
            leader_start: super::start_time(pgid).unwrap(),
            leader_birth: None,
            members: None,
        };
        let super::Ownership::Owned(read) = super::ownership(&group).unwrap() else {
            panic!("the running leader proves its group");
        };
        let [leader] = read.as_slice() else {
            panic!("the leader alone runs in the group: {read:?}");
        };
        let stale = leader.version;

        writeln!(job.0.stdin.as_mut().unwrap(), "go").unwrap();
        while super::Process::observe(pgid)
            .unwrap()
            .is_some_and(|now| now.version == stale)
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            super::group_has_live_members(pgid).unwrap(),
            "the leader runs on as `cat` under a new version"
        );

        assert_eq!(
            super::end_running(&[(group, read)], Duration::ZERO).unwrap(),
            (vec![6], Vec::new())
        );
        assert!(
            !super::group_has_live_members(pgid).unwrap(),
            "the exec'd leader was left running"
        );
    }

    /// A member's record is read only through the identity it was found with: under any other
    /// version of its pid -- the shape a reused pid takes -- the census finds no process, so a
    /// stranger is never charged the job's CPU or named its leaf.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_census_reads_only_the_member_its_identity_names() {
        use crate::api::process::JobLeafAttribution;
        use crate::api::resources::{CpuMicros, CpuTotals};
        use crate::runtime::job_attribution::{Census, observe};
        use crate::runtime::supervisor::LeafCoverage;

        let mut job = started_job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let members = super::members_of(pgid).unwrap();
        let leader = members
            .iter()
            .find(|member| member.pid() == pgid)
            .expect("the running leader holds its group id");
        let stranger = super::Process {
            pid: leader.pid,
            version: leader.version.wrapping_add(1),
        };
        assert_eq!(stranger.record().unwrap(), None);
        assert_eq!(
            leader.record().unwrap().map(|record| record.pid),
            Some(leader.pid.cast_unsigned())
        );
        // One microsecond accounted: any own CPU of an identified member covers all of it.
        let total = CpuTotals {
            user_us: CpuMicros::new(1).unwrap(),
            sys_us: CpuMicros::new(0).unwrap(),
        };
        let coverage = LeafCoverage::new(900).unwrap();
        let leader_pid = leader.pid.cast_unsigned();

        let mut census = Census::led_by(leader_pid, std::time::Instant::now());
        observe(
            &mut census,
            std::slice::from_ref(&stranger),
            std::time::Instant::now(),
        )
        .unwrap();
        assert_eq!(
            census.attribute(total, coverage).unwrap().leaf,
            JobLeafAttribution::Unattributed {
                observed_cpu_permille: 0,
                required_permille: 900,
            },
            "another life of the pid is no member"
        );

        let mut census = Census::led_by(leader_pid, std::time::Instant::now());
        observe(
            &mut census,
            std::slice::from_ref(leader),
            std::time::Instant::now(),
        )
        .unwrap();
        let JobLeafAttribution::Attributed { leaf, .. } =
            census.attribute(total, coverage).unwrap().leaf
        else {
            panic!("the member itself is observed");
        };
        assert_eq!(leaf.pid, leader_pid);

        assert!(leader.signal(libc::SIGKILL).unwrap());
        job.wait_bounded().unwrap();
        end_started_group(pgid);
    }

    /// A process's exit is watched only while it runs: a watch made once it exited -- here, after
    /// it was reaped, when its pid could already name another process -- is refused, never taken
    /// as its exit. One made while it ran sees it exit, and not before.
    #[cfg(target_os = "macos")]
    #[test]
    fn an_exit_is_watched_only_from_while_the_process_ran() {
        let mut job = started_job_group();
        let pgid = i32::try_from(job.id()).unwrap();
        let members = super::members_of(pgid).unwrap();
        let leader = members
            .iter()
            .find(|member| member.pid() == pgid)
            .expect("the running leader holds its group id");
        let exit = leader.watch_exit().expect("a running process is watched");
        assert!(!exit.within(Duration::from_millis(50)).unwrap());

        assert!(leader.signal(libc::SIGTERM).unwrap());
        exit.wait().unwrap();
        job.wait_bounded().unwrap();
        assert!(
            leader.watch_exit().is_err(),
            "a reaped process's exit was watched"
        );
        end_started_group(pgid);
    }

    /// Once a group's leader is reaped, the processes holding its id cannot be told from a
    /// stranger's group that took the id after the job's group ended: here a group whose leader
    /// exited and was reaped, leaving a descendant that holds the id. Whatever identity the ledger
    /// recorded -- this build's start time, an earlier build's birth time, or none -- recovery
    /// never signals it. The next supervisor takes it over as unresolved and every later ledger
    /// carries it until nothing holds the id; only then is its record dropped.
    #[cfg(target_os = "macos")]
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

    /// A pid another user's process now holds is told from the recorded leader by its start
    /// time, which the kernel answers whoever owns the process: a start that differs names
    /// another process, so the group is not the job's, and a lost supervisor's ledger naming it is
    /// taken without a signal or a refusal. A start that matches is inspected: every member's
    /// identity is public whoever owns it (11_shell.md), so the group is read, and signalling
    /// another user's process stays the kernel's refusal. An earlier build's rusage birth cannot
    /// be compared at all -- the kernel declines another user's rusage -- and stays a refusal.
    /// launchd (pid 1, root) stands in for the recycled pid.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_pid_another_user_now_holds_is_told_from_the_leader_by_its_start_time() {
        const HOLDER: i32 = 1;
        let started = super::start_time(HOLDER)
            .unwrap()
            .expect("launchd holds pid 1");
        assert_eq!(
            super::birth_time(HOLDER).unwrap_err().raw_os_error(),
            Some(libc::EPERM),
            "the kernel declines this user another user's rusage"
        );
        let group = |leader_start, leader_birth| super::Group {
            job_id: 68,
            pgid: HOLDER,
            leader_start,
            leader_birth,
            members: None,
        };
        assert!(matches!(
            super::ownership(&group(Some(started + 1), None)).unwrap(),
            super::Ownership::NotOwned
        ));
        let super::Ownership::Owned(members) = super::ownership(&group(Some(started), None))
            .expect("a holder whose start matches the record is inspected")
        else {
            panic!("a holder whose start matches the record is never released uninspected");
        };
        let launchd = members
            .iter()
            .find(|member| member.pid() == HOLDER)
            .expect("launchd holds its own group");
        assert_eq!(
            launchd.signal(libc::SIGCONT).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied,
            "the kernel refuses this user a signal to another user's process"
        );
        let Err(legacy) = super::ownership(&group(None, Some(3_131_429_805_919))) else {
            panic!("an earlier build's birth was compared against another user's process");
        };
        assert_eq!(legacy.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(
            legacy
                .to_string()
                .contains("against its recorded rusage birth from an earlier build"),
            "{legacy}"
        );

        let (_scratch, ledger) = scratch();
        super::replace(
            &ledger,
            &super::Ledger {
                supervisor: std::process::id() + 1,
                ended: true,
                groups: vec![group(Some(started + 1), None)],
                unresolved: Vec::new(),
            },
        )
        .unwrap();
        assert!(take_lost(&ledger, Duration::ZERO).unwrap().is_empty());
        assert!(!ledger.exists());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn permission_denials_preserve_the_owned_ledger_and_allow_retry() {
        const LEDGER_ENV: &str = "COWSHED_TEST_GROUP_LEDGER";
        if let Some(path) = std::env::var_os(LEDGER_ENV) {
            let path = std::path::PathBuf::from(path);
            let error = end_recorded(&path, Writer::Any, Duration::ZERO)
                .expect_err("denied group authority cannot mark the ledger ended");
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            let error = take_lost(&path, Duration::ZERO)
                .expect_err("denied group authority cannot remove the ledger");
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            return;
        }

        for (profile, previously_ended) in [
            ("(version 1)(allow default)(deny signal)", false),
            (
                "(version 1)(allow default)(deny process-info*)(deny signal)",
                false,
            ),
            ("(version 1)(allow default)(deny signal)", true),
        ] {
            let group = OwnedGroup(job_group());
            let pgid = i32::try_from(group.0.id()).unwrap();
            let (_scratch, ledger) = scratch();
            super::Recorder::new(ledger.clone())
                .record(
                    [super::JobGroupClaim::group(7, leader(group.0.id()))].into_iter(),
                    &[],
                )
                .unwrap();
            if previously_ended {
                let mut recorded = super::read(&ledger).unwrap().unwrap();
                recorded.ended = true;
                super::replace(&ledger, &recorded).unwrap();
            }
            let before = std::fs::read(&ledger).unwrap();
            let output = Command::new("/usr/bin/sandbox-exec")
                .args(["-p", profile, "--"])
                .arg(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime::job_groups::tests::permission_denials_preserve_the_owned_ledger_and_allow_retry",
                    "--nocapture",
                ])
                .env(LEDGER_ENV, &ledger).output_bounded()
                .expect("real restricted cleanup process");
            let after = std::fs::read(&ledger).unwrap();
            let survived_refusal = group_alive(pgid);
            let retried = end_recorded(&ledger, Writer::Any, Duration::ZERO);
            let taken = take_lost(&ledger, Duration::ZERO);
            drop(group);

            assert!(
                output.status.success(),
                "restricted cleanup: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(after, before, "failed cleanup retains the original ledger");
            assert!(survived_refusal, "the refused group remained alive");
            assert_eq!(
                retried.unwrap().signalled,
                vec![7],
                "host authority can retry cleanup"
            );
            taken.unwrap();
            assert!(!ledger.exists(), "successful retry consumes the ledger");
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
