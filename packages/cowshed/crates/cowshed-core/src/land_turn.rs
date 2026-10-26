//! Land turns: one land of a repository at a time, in the order the lands asked (05_gateway.md
//! "Land turns").
//!
//! A bare `flock(2)` on one lock file serialized lands but never ordered them: every waiter is
//! woken when the holder releases and the first to retry wins, so a land that asks at the moment
//! of a release takes the turn ahead of one that has waited for minutes. Measured 2026-10-21: a
//! 72-commit train queued behind one land, and when that land released, a land that had asked
//! after the train took the turn and held it for 36 minutes.
//!
//! So a land takes a numbered ticket and waits for its turn. Each ticket is a file in the
//! repository's directory, exclusively locked for as long as the land or any process handed its
//! descriptor holds or waits for the turn. The kernel drops the lock when the last inherited
//! description closes; a ticket whose lock is free is dead and is removed by whoever finds it.
//! Numbers are given out under the directory's `.seq` lock, each above every ticket present, so
//! the live tickets are in the order their lands asked. The lowest number holds the turn; every other
//! waits on the lock of the live ticket just ahead of it and, when that ends, looks again. Nothing
//! polls, and nobody but the next in line is woken.
//!
//! The head then takes the outside lock, the file lands took before every caller asked for turns.
//! A process outside the queue holding it records nothing, so while the head waits it names the
//! processes with that file open from one descriptor census per report. Measured 2026-10-21: a
//! land waited 1002 s behind an outside holder having said once that "a land that did not ask
//! for a turn" held it, and nothing named the process.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd as _, RawFd};
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cowshed_os::process::Holder;
use serde::{Deserialize, Serialize};

use crate::held_lock::{HeldLock, Opened};

/// The directory holding every repository's land turns: beside the install slot, in the host's
/// support directory, which only this user writes.
pub fn land_turn_root(support_directory: &Path) -> PathBuf {
    support_directory.join("land-turn")
}

/// One land's place in its repository's queue, as its ticket file records it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Ticket {
    /// The order the land asked in: lower holds the turn first.
    pub number: u64,
    /// The process that waits for or holds the turn.
    pub pid: u32,
    /// The workspace the land lands.
    pub workspace: String,
    /// When the land asked, in milliseconds since the Unix epoch.
    pub asked_unix_ms: u64,
    /// The command the turn runs.
    pub command: String,
}

/// What a waiting land is told: who holds the turn and how many live tickets are ahead of its own.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Wait {
    /// Behind `ahead` live tickets, the first of which, `holder`, holds the turn.
    Queued { holder: Ticket, ahead: usize },
    /// At the head, waiting for `lock`, which a process outside the queue holds: a land started
    /// before every caller asked for turns, or anything else that took the file.
    Outside {
        lock: PathBuf,
        holders: OutsideHolders,
    },
}

/// Who holds the outside lock, as one census of the processes with that file open shows it.
/// Darwin names no `flock(2)` owner, so an open descriptor is the evidence, as for `lsof`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutsideHolders {
    /// The processes other than the waiting land with the file open, by pid, each once: the
    /// holder is one of them.
    Open(Vec<Holder>),
    /// No other process has it open: the holder let go as the census ran, or is not this user's
    /// to see.
    NoneOpen,
    /// The census could not be taken, and why.
    Unknown(String),
}

/// What the land `me`, waiting for the outside lock with its own descriptor open on it, says holds
/// that lock, given one census `open` of the processes with the file open. Pure: the census is
/// the caller's.
pub fn outside_holders(me: i32, open: Result<Vec<Holder>, String>) -> OutsideHolders {
    match open {
        Err(error) => OutsideHolders::Unknown(error),
        Ok(mut open) => {
            open.retain(|holder| holder.pid != me);
            open.sort_by_key(|holder| holder.pid);
            open.dedup_by_key(|holder| holder.pid);
            if open.is_empty() {
                OutsideHolders::NoneOpen
            } else {
                OutsideHolders::Open(open)
            }
        }
    }
}

/// A held turn. Ending it records how the land's command ended. The ticket remains discoverable
/// until every inherited holder closes it; a later queue scan removes it once its lock is free.
/// A turn dropped without an explicit end is then recorded as abandoned, not finished.
#[derive(Debug)]
pub struct Turn {
    directory: PathBuf,
    ticket: Ticket,
    held_ticket: File,
    outside: Option<HeldLock>,
}

impl Turn {
    /// End the turn, recording `outcome` (how the command ended) in the repository's turn log.
    pub fn end(self, outcome: String) {
        // The original JSON has no trailing newline. Append one byte as the completion marker:
        // every prefix is still valid ticket JSON, including a concurrent reader's snapshot.
        // The inherited description's cursor is not ours, so never write through that cursor.
        let marked = self
            .held_ticket
            .metadata()
            .and_then(|metadata| self.held_ticket.write_all_at(b"\n", metadata.len()));
        if let Err(error) = marked {
            cowshed_os::eprintln!(
                "cowshed: land turn {}: ticket {} completion marker not written: {error}",
                self.directory.display(),
                self.ticket.number,
            );
        }
        // Mark first: a failed log append must not turn an explicitly ended ticket into an
        // abandonment on the next scan. Neither operation releases an inherited holder.
        record(
            &self.directory,
            &self.ticket,
            TurnEventKind::Finished,
            Some(outcome),
        );
    }

    /// The descriptor of this turn's ticket alone, for a command that holds the turn and no outside
    /// lock ([`crate::runtime::ref_turn::RefTurn`]).
    pub fn ticket_descriptor(&self) -> RawFd {
        self.held_ticket.as_raw_fd()
    }

    /// The descriptors that hold this turn: its ticket's lock and the outside lock's. A command
    /// handed them ([`crate::held_lock::keep_across_exec`]) holds the turn with this process, so
    /// the kernel keeps the turn until the command has ended too, however this process ends: no
    /// command runs outside its turn.
    pub fn descriptors(&self) -> Vec<RawFd> {
        std::iter::once(self.held_ticket.as_raw_fd())
            .chain(self.outside.as_ref().map(HeldLock::descriptor))
            .collect()
    }
}

const TICKET_SUFFIX: &str = ".ticket";
const SEQ_NAME: &str = ".seq";
/// The repository's turn log: one JSON line per ticket event, appended, never rewritten.
const LOG_NAME: &str = "turns.log";

/// What happened to a ticket, as the turn log records it.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TurnEventKind {
    /// The land asked and took its place in line.
    Queued,
    /// The land holds the turn.
    Taken,
    /// The land's command ended; `outcome` says how.
    Finished,
    /// The land ended without recording an outcome, found unlocked by whoever read the queue
    /// next. It may have been dropped on an error, killed, crashed, or lost with its session.
    Abandoned,
}

/// One line of a repository's turn log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnEvent {
    /// When it was recorded, in milliseconds since the Unix epoch.
    pub at_unix_ms: u64,
    pub event: TurnEventKind,
    pub ticket: Ticket,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
}

fn now_unix_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

/// Append one event to `directory`'s turn log. A log that cannot be written is said on stderr
/// and never stops a land: the turn is the kernel's, the log its account.
fn record(directory: &Path, ticket: &Ticket, event: TurnEventKind, outcome: Option<String>) {
    let line = TurnEvent {
        at_unix_ms: now_unix_ms(),
        event,
        ticket: ticket.clone(),
        outcome,
    };
    let written = serde_json::to_vec(&line)
        .map_err(io::Error::other)
        .and_then(|mut bytes| {
            bytes.push(b'\n');
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(directory.join(LOG_NAME))?
                .write_all(&bytes)
        });
    if let Err(error) = written {
        cowshed_os::eprintln!(
            "cowshed: land turn {}: ticket {} {event:?} not recorded in {}: {error}",
            directory.display(),
            ticket.number,
            directory.join(LOG_NAME).display()
        );
    }
}

/// The `abandoned` events of `directory`'s turn log recorded at or after `since_unix_ms`, oldest
/// first.
pub fn abandoned_since(directory: &Path, since_unix_ms: u64) -> io::Result<Vec<TurnEvent>> {
    let text = match fs::read_to_string(directory.join(LOG_NAME)) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut abandoned = Vec::new();
    for line in text.lines() {
        let event: TurnEvent = serde_json::from_str(line).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "land turn log {}: {error}",
                    directory.join(LOG_NAME).display()
                ),
            )
        })?;
        if event.event == TurnEventKind::Abandoned && event.at_unix_ms >= since_unix_ms {
            abandoned.push(event);
        }
    }
    Ok(abandoned)
}

fn ticket_name(number: u64) -> String {
    format!("{number:020}{TICKET_SUFFIX}")
}

fn ticket_number(name: &str) -> Option<u64> {
    name.strip_suffix(TICKET_SUFFIX)?.parse().ok()
}

/// Every ticket file in `directory` by number, lowest first, live or not.
fn ticket_files(directory: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut tickets = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if let Some(number) = entry.file_name().to_str().and_then(ticket_number) {
            tickets.push((number, entry.path()));
        }
    }
    tickets.sort_unstable_by_key(|(number, _)| *number);
    Ok(tickets)
}

/// A ticket file opened to read, or `None` once it is gone.
fn open_ticket(path: &Path) -> io::Result<Option<File>> {
    match File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// What a reader's shared `try_lock` on a ticket answers: its land, or a process the land handed
/// its descriptor to, still holds it, or nobody does and the ticket is dead. The kernel's lock is
/// the only evidence of a ticket's death: no pid a ticket records is ever consulted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TicketLock {
    Held,
    Free,
}

/// One ticket file as a reader found it: the kernel's answer for its lock, and its record with
/// whether its land ended it (the completion marker), or why the record could not be read.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Found {
    lock: TicketLock,
    record: Result<(Ticket, bool), String>,
}

/// What a reader does with one ticket it found.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Verdict {
    /// Its lock is held: the ticket is in line, in its number's place.
    Live(Ticket),
    /// Its lock is free and no completion marker was written: its land ended without recording an
    /// outcome. Recorded abandoned, then removed.
    Abandoned(Ticket),
    /// Its lock is free and its land ended it, or its record cannot be read: removed.
    Removed,
}

/// The verdict on one found ticket. A held ticket whose record cannot be read is an error: its
/// land is alive and its place unknowable. Pure.
fn judge(found: Found) -> Result<Verdict, String> {
    match (found.lock, found.record) {
        (TicketLock::Held, Ok((ticket, _))) => Ok(Verdict::Live(ticket)),
        (TicketLock::Held, Err(reason)) => Err(reason),
        (TicketLock::Free, Ok((ticket, false))) => Ok(Verdict::Abandoned(ticket)),
        (TicketLock::Free, Ok((_, true)) | Err(_)) => Ok(Verdict::Removed),
    }
}

/// A land's place behind the live tickets numbered below its own.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Place {
    /// The lowest live ticket: it holds the turn.
    holder: Ticket,
    /// How many live tickets are ahead.
    ahead: usize,
    /// The live ticket just ahead, whose end is the only one this land waits for.
    nearest: u64,
}

/// Where the land holding ticket `number` stands in `live`, the live tickets in any order, or
/// `None` at the head. Pure.
fn place(live: &[Ticket], number: u64) -> Option<Place> {
    let mut ahead: Vec<&Ticket> = live.iter().filter(|other| other.number < number).collect();
    ahead.sort_unstable_by_key(|other| other.number);
    Some(Place {
        holder: (*ahead.first()?).clone(),
        ahead: ahead.len(),
        nearest: ahead.last()?.number,
    })
}

/// What the kernel and the file say of the ticket `file` opened at `path`. A free lock is taken
/// shared and stays taken while `file` is open, so a dead ticket is removed under it and no live
/// one ever is.
fn found(file: &File, path: &Path) -> io::Result<Found> {
    let lock = match file.try_lock_shared() {
        Ok(()) => TicketLock::Free,
        Err(TryLockError::WouldBlock) => TicketLock::Held,
        Err(TryLockError::Error(error)) => return Err(error),
    };
    Ok(Found {
        lock,
        record: read_ticket(file, path).map_err(|error| error.to_string()),
    })
}

/// Carry out `verdict` on the ticket at `path` in `directory`, still holding its file open; the
/// live ticket, if it is one.
fn settle(directory: &Path, path: &Path, verdict: Verdict) -> io::Result<Option<Ticket>> {
    let removed = match verdict {
        Verdict::Live(ticket) => return Ok(Some(ticket)),
        Verdict::Abandoned(ticket) => {
            record(
                directory,
                &ticket,
                TurnEventKind::Abandoned,
                Some("its land ended without recording an outcome".to_owned()),
            );
            fs::remove_file(path)
        }
        Verdict::Removed => fs::remove_file(path),
    };
    match removed {
        Ok(()) => Ok(None),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// The live ticket at `path` in `directory`, judging and settling it; `None` once it is gone.
fn read_live(directory: &Path, path: &Path) -> io::Result<Option<(Ticket, File)>> {
    let Some(file) = open_ticket(path)? else {
        return Ok(None);
    };
    let verdict = judge(found(&file, path)?)
        .map_err(|reason| io::Error::new(io::ErrorKind::InvalidData, reason))?;
    Ok(settle(directory, path, verdict)?.map(|ticket| (ticket, file)))
}

fn read_ticket(mut file: &File, path: &Path) -> io::Result<(Ticket, bool)> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let finished = bytes.last() == Some(&b'\n');
    serde_json::from_slice(&bytes)
        .map(|ticket| (ticket, finished))
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("land turn ticket {}: {error}", path.display()),
            )
        })
}

/// The live tickets of `directory` in turn order, the first holding the turn; dead ones found on
/// the way are removed. Takes no turn: `gateway status` reads queues this way.
pub fn observe(directory: &Path) -> io::Result<Vec<Ticket>> {
    let tickets = match ticket_files(directory) {
        Ok(tickets) => tickets,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut queue = Vec::new();
    for (_, path) in tickets {
        if let Some((ticket, _)) = read_live(directory, &path)? {
            queue.push(ticket);
        }
    }
    Ok(queue)
}

/// A repository's live tickets, the holder first, and the tickets recently found abandoned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepoTurns {
    pub repo: String,
    pub tickets: Vec<Ticket>,
    pub abandoned: Vec<TurnEvent>,
}

/// Every repository with a live ticket or, when `abandoned_since_unix_ms` is given, a ticket
/// abandoned at or after it, as `observe` and `abandoned_since` read each. `None` reads no
/// repository's log: the live tickets alone.
pub fn observe_all(
    root: &Path,
    abandoned_since_unix_ms: Option<u64>,
) -> io::Result<Vec<RepoTurns>> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut repositories = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(repo) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        // Read the queue first: a dead ticket it finds is recorded as abandoned, and then shown.
        let tickets = observe(&entry.path())?;
        let abandoned = match abandoned_since_unix_ms {
            Some(since) => abandoned_since(&entry.path(), since)?,
            None => Vec::new(),
        };
        if !tickets.is_empty() || !abandoned.is_empty() {
            repositories.push(RepoTurns {
                repo,
                tickets,
                abandoned,
            });
        }
    }
    repositories.sort_unstable_by(|left, right| left.repo.cmp(&right.repo));
    Ok(repositories)
}

/// Take a ticket under the `.seq` lock, numbered above every ticket present, and lock it before
/// any other land can read it: written under a temporary name, then renamed into place.
fn take_ticket(directory: &Path, workspace: &str, command: &str) -> io::Result<(Ticket, File)> {
    let seq = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(directory.join(SEQ_NAME))?;
    // WHY unbounded: debt: another land holds `.seq` only while it numbers one ticket; a holder
    // stalled in the kernel stalls this. Moves to cowshed_os::flock.
    #[allow(clippy::disallowed_methods)]
    seq.lock()?;
    // A temporary ticket nobody locks is one whose taker died before renaming it: remove it.
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.ends_with(".tmp"))
        {
            fs::remove_file(entry.path())?;
        }
    }
    let number = ticket_files(directory)?
        .last()
        .map_or(1, |(number, _)| number + 1);
    let ticket = Ticket {
        number,
        pid: std::process::id(),
        workspace: workspace.to_owned(),
        asked_unix_ms: u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX),
        command: command.to_owned(),
    };
    let staged = directory.join(format!(".{number:020}.tmp"));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&staged)?;
    // A file this process just created is locked by nobody else: the lock is taken at once.
    file.try_lock().map_err(io::Error::from)?;
    file.write_all(&serde_json::to_vec(&ticket).map_err(io::Error::other)?)?;
    let path = directory.join(ticket_name(number));
    fs::rename(&staged, &path)?;
    drop(seq);
    record(directory, &ticket, TurnEventKind::Queued, None);
    Ok((ticket, file))
}

/// The live ticket just ahead of `number`, held open, and this land's place, or `None` at the head.
fn ahead_of(directory: &Path, number: u64) -> io::Result<Option<(File, Place)>> {
    let mut live = Vec::new();
    for (_, path) in ticket_files(directory)?
        .into_iter()
        .filter(|(other, _)| *other < number)
    {
        if let Some(found) = read_live(directory, &path)? {
            live.push(found);
        }
    }
    let tickets: Vec<Ticket> = live.iter().map(|(ticket, _)| ticket.clone()).collect();
    let Some(place) = place(&tickets, number) else {
        return Ok(None);
    };
    let nearest = live
        .into_iter()
        .find(|(ticket, _)| ticket.number == place.nearest)
        .map(|(_, file)| file)
        .expect("the nearest live ticket is one this read found");
    Ok(Some((nearest, place)))
}

/// What a waiting land does next: say where it stands now, or keep waiting up to the time left
/// before its next report is due.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Report {
    Now,
    After(Duration),
}

/// When a waiting land reports, given how long ago it last did (`None`: it has not yet) and its
/// cadence `every`: at once when it starts to wait, and again each time `every` has passed.
/// Pure: the caller's clock is the only input of time.
fn report(since_told: Option<Duration>, every: Duration) -> Report {
    match since_told {
        Some(since) if since < every => Report::After(every - since),
        None | Some(_) => Report::Now,
    }
}

/// Run `blocking` on a thread of its own and answer what it answers, calling `tell` once it has
/// started and every `every` until then ([`report`]): a lock wait that keeps saying what it waits
/// for. The wait starts first, so however long a `tell` takes, it never delays taking the lock.
fn wait_telling<T: Send + 'static>(
    blocking: impl FnOnce() -> io::Result<T> + Send + 'static,
    every: Duration,
    mut tell: impl FnMut() -> io::Result<()>,
) -> io::Result<T> {
    let (ended, end) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = ended.send(blocking());
    });
    let mut told: Option<std::time::Instant> = None;
    loop {
        if report(told.map(|at| at.elapsed()), every) == Report::Now {
            tell()?;
            told = Some(std::time::Instant::now());
        }
        let left = match report(told.map(|at| at.elapsed()), every) {
            Report::After(left) => left,
            Report::Now => Duration::ZERO,
        };
        match end.recv_timeout(left) {
            Ok(result) => return result,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(io::Error::other(
                    "the land turn wait thread ended unanswered",
                ));
            }
        }
    }
}

/// The outside wait of the land `me`, from one census of the processes with `lock` open
/// (05_gateway.md "Land turns"; the one holder census 02_workspaces.md admits outside a
/// destructive operation, "Holders cowshed does not run"). It reports and decides nothing.
fn outside_wait(lock: &Path, me: i32) -> Wait {
    let open = crate::build_volume::nx::holders(lock).map_err(|error| error.to_string());
    Wait::Outside {
        lock: lock.to_path_buf(),
        holders: outside_holders(me, open),
    }
}

/// Wait in `directory`'s queue for this process's turn to land `workspace` with `command`, then
/// take `outside` exclusively too when given: the lock file of lands that do not ask for turns,
/// kept and never removed. `waiting` is told who holds the turn and how many are ahead, or which
/// processes have the outside lock open, whenever that changes and every `report_every` while it
/// does not. The turn is held until the last inherited copy of its ticket descriptor closes.
pub fn take(
    directory: &Path,
    workspace: &str,
    command: &str,
    outside: Option<&Path>,
    report_every: Duration,
    mut waiting: impl FnMut(&Wait),
) -> io::Result<Turn> {
    fs::create_dir_all(directory)?;
    let (ticket, file) = take_ticket(directory, workspace, command)?;
    while let Some((nearest, standing)) = ahead_of(directory, ticket.number)? {
        let mut told = Wait::Queued {
            holder: standing.holder,
            ahead: standing.ahead,
        };
        // The wait on the ticket just ahead runs on its own thread, so this one can repeat what
        // it was told at its cadence; the lock is released at once, since only its end matters.
        // The first telling is what `ahead_of` just read; each later one looks again.
        let mut first = true;
        // WHY unbounded: debt: a land waits for the land ahead for as long as that land's turn runs,
        // telling its place and the holder at its cadence; a land ahead that wedges holding its
        // ticket stalls this one until the operator interrupts. Moves to cowshed_os::flock.
        #[allow(clippy::disallowed_methods)]
        wait_telling(
            move || nearest.lock_shared(),
            report_every,
            || {
                if !first {
                    let now = observe(directory)?;
                    if let Some(place) = place(&now, ticket.number) {
                        told = Wait::Queued {
                            holder: place.holder,
                            ahead: place.ahead,
                        };
                    }
                }
                first = false;
                waiting(&told);
                Ok(())
            },
        )?;
    }
    // The outside lock is the held lock every other holder of it takes, recorded on its inode.
    let outside = match outside {
        None => None,
        Some(lock) => Some(match Opened::open(lock)?.try_hold()? {
            Ok(held) => held,
            Err(opened) => {
                let me = i32::try_from(std::process::id()).map_err(io::Error::other)?;
                wait_telling(
                    move || opened.hold(),
                    report_every,
                    || {
                        waiting(&outside_wait(lock, me));
                        Ok(())
                    },
                )?
            }
        }),
    };
    record(directory, &ticket, TurnEventKind::Taken, None);
    Ok(Turn {
        directory: directory.to_path_buf(),
        ticket,
        held_ticket: file,
        outside,
    })
}

#[cfg(test)]
mod tests {
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;

    fn events(directory: &Path) -> Vec<TurnEvent> {
        fs::read_to_string(directory.join(LOG_NAME))
            .expect("read turn history")
            .lines()
            .map(|line| serde_json::from_str(line).expect("a turn event"))
            .collect()
    }

    fn ticket(number: u64, workspace: &str) -> Ticket {
        Ticket {
            number,
            pid: 4000 + u32::try_from(number).expect("a small ticket number"),
            workspace: workspace.to_owned(),
            asked_unix_ms: 1_793_000_000_000 + number,
            command: "land".to_owned(),
        }
    }

    /// Every lock answer against every record: only a held lock keeps a ticket in line, whatever
    /// its record says (a dead land's pid is never asked); a free one ended without its marker is
    /// abandoned, a free one its land ended or whose record cannot be read is only removed, and a
    /// held one whose record cannot be read is an error, never a guess at its place.
    #[test]
    fn only_a_held_lock_keeps_a_ticket_in_line_and_an_unmarked_dead_one_is_abandoned() {
        let unread = || Err("land turn ticket: EOF while parsing".to_owned());
        let cases = [
            (
                TicketLock::Held,
                Ok((ticket(1, "H"), false)),
                Ok(Verdict::Live(ticket(1, "H"))),
            ),
            (
                TicketLock::Held,
                Ok((ticket(1, "H"), true)),
                Ok(Verdict::Live(ticket(1, "H"))),
            ),
            (
                TicketLock::Held,
                unread(),
                Err("land turn ticket: EOF while parsing".to_owned()),
            ),
            (
                TicketLock::Free,
                Ok((ticket(2, "W1"), false)),
                Ok(Verdict::Abandoned(ticket(2, "W1"))),
            ),
            (
                TicketLock::Free,
                Ok((ticket(2, "W1"), true)),
                Ok(Verdict::Removed),
            ),
            (TicketLock::Free, unread(), Ok(Verdict::Removed)),
        ];
        for (lock, record, expected) in cases {
            assert_eq!(
                judge(Found {
                    lock,
                    record: record.clone()
                }),
                expected,
                "{lock:?} {record:?}"
            );
        }
    }

    /// The lowest live ticket holds the turn; a land waits only on the live ticket just below its
    /// own, behind every live one below it, and is at the head when none is below. Tickets above,
    /// its own, and the gaps dead tickets left decide nothing; the order the reader found them in
    /// decides nothing either.
    #[test]
    fn a_land_waits_on_the_nearest_live_ticket_behind_the_lowest() {
        let live = [
            ticket(3, "W2"),
            ticket(1, "H"),
            ticket(7, "W3"),
            ticket(2, "W1"),
        ];
        let at = |number| place(&live, number);
        assert_eq!(at(1), None, "the lowest ticket is the head");
        assert_eq!(
            at(0),
            None,
            "nothing is below a ticket lower than every live one"
        );
        for (number, ahead, nearest) in [(2, 1, 1), (3, 2, 2), (5, 3, 3), (7, 3, 3), (9, 4, 7)] {
            assert_eq!(
                at(number),
                Some(Place {
                    holder: ticket(1, "H"),
                    ahead,
                    nearest,
                }),
                "ticket {number}"
            );
        }
        assert_eq!(place(&[], 4), None, "an empty line has no holder");
    }

    /// A waiting land reports when it starts to wait and each time its cadence has passed since its
    /// last report, and otherwise waits exactly the time left; no other time decides it.
    #[test]
    fn a_waiting_land_reports_at_once_then_once_per_cadence() {
        let every = Duration::from_secs(90);
        for (since, expected) in [
            (None, Report::Now),
            (Some(Duration::ZERO), Report::After(every)),
            (
                Some(Duration::from_millis(89_999)),
                Report::After(Duration::from_millis(1)),
            ),
            (Some(every), Report::Now),
            (Some(Duration::from_secs(400)), Report::Now),
        ] {
            assert_eq!(report(since, every), expected, "{since:?}");
        }
    }

    /// The head releases while earlier waiters still hold their tickets and a late land arrives:
    /// the late land waits behind every earlier held ticket, on the nearest of them, never passing
    /// them however soon the head released; a waiter killed in line is abandoned and the late land
    /// then waits on the one left. Each step is one reader's observations of the line.
    #[test]
    fn a_late_arrival_waits_behind_every_earlier_held_ticket_after_the_head_releases() {
        let found = |number, workspace, lock, ended| Found {
            lock,
            record: Ok((ticket(number, workspace), ended)),
        };
        let read = |sightings: Vec<Found>| -> Vec<Ticket> {
            sightings
                .into_iter()
                .filter_map(
                    |sighting| match judge(sighting).expect("a readable ticket") {
                        Verdict::Live(ticket) => Some(ticket),
                        Verdict::Abandoned(_) | Verdict::Removed => None,
                    },
                )
                .collect()
        };
        let (held, free) = (TicketLock::Held, TicketLock::Free);

        // H holds the turn; W1 and W2 wait behind it.
        let line = read(vec![
            found(1, "H", held, false),
            found(2, "W1", held, false),
            found(3, "W2", held, false),
        ]);
        assert_eq!(
            place(&line, 2),
            Some(Place {
                holder: ticket(1, "H"),
                ahead: 1,
                nearest: 1,
            })
        );
        assert_eq!(
            place(&line, 3),
            Some(Place {
                holder: ticket(1, "H"),
                ahead: 2,
                nearest: 2,
            })
        );

        // H ended its turn; W3 asks while W1 and W2 still hold theirs.
        let line = read(vec![
            found(1, "H", free, true),
            found(2, "W1", held, false),
            found(3, "W2", held, false),
            found(4, "W3", held, false),
        ]);
        assert_eq!(place(&line, 2), None, "W1 is the head once H has ended");
        assert_eq!(
            place(&line, 4),
            Some(Place {
                holder: ticket(2, "W1"),
                ahead: 2,
                nearest: 3,
            }),
            "the late land waits behind both earlier tickets, on the nearest"
        );

        // W1 is killed in line: its lock dies unmarked, so it is abandoned, and W3 waits on W2.
        assert_eq!(
            judge(found(2, "W1", free, false)),
            Ok(Verdict::Abandoned(ticket(2, "W1")))
        );
        let line = read(vec![
            found(2, "W1", free, false),
            found(3, "W2", held, false),
            found(4, "W3", held, false),
        ]);
        assert_eq!(
            place(&line, 4),
            Some(Place {
                holder: ticket(3, "W2"),
                ahead: 1,
                nearest: 3,
            })
        );
    }

    /// The direct child exits, but its descendant retains the ticket until the parent releases a
    /// pipe. The next turn must wait on that ticket, without an outside lock to hide early unlink.
    fn descendant_keeps_turn(finish: bool, exit_code: i32) {
        use cowshed_os::fork_lock::Spawn as _;
        use std::process::{Command, Stdio};

        let root = crate::temp_root::TempRoot::new("land-descendant");
        let turn = take(
            &root,
            "first",
            "foreground",
            None,
            Duration::from_secs(5),
            |_| {
                panic!("the first turn has no predecessor");
            },
        )
        .expect("take first turn");
        let first = turn.ticket.clone();
        let path = root.join(ticket_name(first.number));
        let descendant_done = root.join("descendant-done");
        let (release_read, mut release_write) = io::pipe().expect("descendant release pipe");
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                r#"sh -c 'IFS= read -r release; : > "$1"' descendant "$1" <&0 & exit "$2""#,
                "land-direct",
            ])
            .arg(&descendant_done)
            .arg(exit_code.to_string())
            .stdin(Stdio::from(release_read))
            .stdout(Stdio::null());
        crate::held_lock::hand_to(&mut command, turn.descriptors());
        let status = command
            .spawn_locked()
            .expect("start direct child")
            .wait_bounded()
            .expect("reap direct child, not its descendant");
        assert_eq!(status.code(), Some(exit_code));

        if finish {
            turn.end(format!("exit {exit_code}"));
        } else {
            drop(turn);
        }
        assert!(
            path.exists(),
            "the descendant's locked inode must stay discoverable"
        );
        assert_eq!(
            observe(&root)
                .expect("observe the descendant's turn")
                .as_slice(),
            std::slice::from_ref(&first)
        );
        assert!(
            abandoned_since(&root, 0)
                .expect("read abandonments")
                .is_empty()
        );

        let (waiting_tx, waiting_rx) = mpsc::channel();
        let (taken_tx, taken_rx) = mpsc::channel();
        let directory = root.to_path_buf();
        let waiter = std::thread::spawn(move || {
            let mut first_wait = Some(waiting_tx);
            let next = take(
                &directory,
                "next",
                "next foreground",
                None,
                Duration::from_millis(20),
                |wait| {
                    if let Some(sender) = first_wait.take() {
                        sender.send(wait.clone()).expect("report the queued turn");
                    }
                },
            );
            taken_tx.send(next).expect("return the next turn");
        });
        assert_eq!(
            waiting_rx.recv().expect("next turn queues"),
            Wait::Queued {
                holder: first.clone(),
                ahead: 1
            },
        );
        assert!(
            matches!(
                File::open(&path)
                    .expect("open the descendant's ticket")
                    .try_lock(),
                Err(std::fs::TryLockError::WouldBlock)
            ),
            "a direct child's exit must not release its descendant's turn",
        );
        assert!(path.exists(), "scanning a locked ticket must not unlink it");
        assert!(
            !descendant_done.exists(),
            "the descendant still waits for release"
        );

        release_write
            .write_all(b"release\n")
            .expect("release descendant");
        drop(release_write);
        let next = taken_rx
            .recv()
            .expect("next turn starts after the descendant closes")
            .expect("take next turn");
        waiter.join().expect("join next turn waiter");
        assert!(descendant_done.exists(), "the descendant reached its end");
        assert!(
            !path.exists(),
            "the unlocked predecessor is cleaned by the queue"
        );
        next.end("exit 0".to_owned());
        assert!(
            observe(&root)
                .expect("clean the ended next turn")
                .is_empty()
        );
        let first_events: Vec<_> = events(&root)
            .into_iter()
            .filter(|event| event.ticket == first)
            .map(|event| (event.event, event.outcome))
            .collect();
        let terminal = if finish {
            (TurnEventKind::Finished, Some(format!("exit {exit_code}")))
        } else {
            (
                TurnEventKind::Abandoned,
                Some("its land ended without recording an outcome".to_owned()),
            )
        };
        assert_eq!(
            first_events,
            [
                (TurnEventKind::Queued, None),
                (TurnEventKind::Taken, None),
                terminal,
            ],
        );
        assert!(ticket_files(&root).expect("ticket cleanup").is_empty());
    }

    #[test]
    fn every_terminal_outcome_waits_for_descendants_after_the_direct_child_exits() {
        for finish in [true, false] {
            for exit_code in [0, 7] {
                descendant_keeps_turn(finish, exit_code);
            }
        }
    }

    #[test]
    fn a_failed_take_leaves_an_unmarked_ticket_for_the_queue_to_clean() {
        let root = crate::temp_root::TempRoot::new("land-take-error");
        let directory = root.join("queue");
        // A directory cannot be opened as a writable outside lock.
        assert!(
            take(
                &directory,
                "failed",
                "never started",
                Some(&root),
                Duration::from_secs(5),
                |_| panic!("the invalid outside lock must fail, not wait"),
            )
            .is_err()
        );
        assert_eq!(
            ticket_files(&directory).expect("error leaves ticket").len(),
            1
        );
        assert!(
            observe(&directory)
                .expect("clean abandoned ticket")
                .is_empty()
        );
        assert!(
            ticket_files(&directory)
                .expect("cleaned tickets")
                .is_empty()
        );
        let kinds: Vec<_> = events(&directory)
            .into_iter()
            .map(|event| event.event)
            .collect();
        assert_eq!(kinds, [TurnEventKind::Queued, TurnEventKind::Abandoned]);
    }

    proptest! {
        /// Whatever the census lists, the outside wait names each process with the lock open but
        /// the waiting land itself, once, by pid ascending, as the first census row of that pid
        /// lists it; no such process is `NoneOpen`, never an empty list; a census that could not
        /// be taken is said with its reason. The reference is the same answer kept in a map.
        #[test]
        fn the_outside_wait_names_every_other_process_with_the_lock_open_once(
            me in 1_i32..16,
            rows in prop::collection::vec((1_i32..16, "[a-z]{1,6}"), 0..12),
            failure in prop::option::of("[a-z ]{1,12}"),
        ) {
            let open: Vec<Holder> = rows
                .into_iter()
                .map(|(pid, command)| Holder { pid, command })
                .collect();
            let census = failure.clone().map_or_else(|| Ok(open.clone()), Err);
            let mut reference = BTreeMap::new();
            for holder in &open {
                if holder.pid != me {
                    reference.entry(holder.pid).or_insert_with(|| holder.clone());
                }
            }
            let expected = match failure {
                Some(reason) => OutsideHolders::Unknown(reason),
                None if reference.is_empty() => OutsideHolders::NoneOpen,
                None => OutsideHolders::Open(reference.into_values().collect()),
            };
            prop_assert_eq!(outside_holders(me, census), expected);
        }
    }
}
