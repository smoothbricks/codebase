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
//! repository's directory, exclusively locked by the process that took it for as long as it holds
//! or waits for the turn: the kernel drops the lock when that process exits however it exits, so
//! a ticket whose lock is free is dead and is removed by whoever finds it. Numbers are given out
//! under the directory's `.seq` lock, each above every ticket present, so the live tickets are in
//! the order their lands asked. The live ticket with the lowest number holds the turn; every other
//! waits on the lock of the live ticket just ahead of it and, when that ends, looks again. Nothing
//! polls, and nobody but the next in line is woken.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

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
    /// At the head, waiting for the lock a land outside the queue holds (a land started before
    /// every caller asked for turns).
    Outside { lock: PathBuf },
}

/// A held turn. Dropping it removes the ticket, then releases its lock.
#[derive(Debug)]
pub struct Turn {
    path: PathBuf,
    _ticket: File,
    _outside: Option<File>,
}

impl Drop for Turn {
    fn drop(&mut self) {
        // Removed while still locked, so no scan in between reads it as a dead ticket.
        let _ = fs::remove_file(&self.path);
    }
}

const TICKET_SUFFIX: &str = ".ticket";
const SEQ_NAME: &str = ".seq";

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

/// Whether `file`'s owner still holds it. A ticket whose lock is free is dead, and is removed
/// while this caller holds that lock shared, so no live ticket is ever removed.
fn live(file: &File, path: &Path) -> io::Result<bool> {
    match file.try_lock_shared() {
        Ok(()) => {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            Ok(false)
        }
        Err(TryLockError::WouldBlock) => Ok(true),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

fn read_ticket(mut file: &File, path: &Path) -> io::Result<Ticket> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    serde_json::from_slice(&bytes).map_err(|error| {
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
        let Some(file) = open_ticket(&path)? else {
            continue;
        };
        if live(&file, &path)? {
            queue.push(read_ticket(&file, &path)?);
        }
    }
    Ok(queue)
}

/// Every repository with a live ticket, and its queue, as `observe` reads each.
pub fn observe_all(root: &Path) -> io::Result<Vec<(String, Vec<Ticket>)>> {
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
        let queue = observe(&entry.path())?;
        if !queue.is_empty() {
            repositories.push((repo, queue));
        }
    }
    repositories.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    Ok(repositories)
}

/// Take a ticket under the `.seq` lock, numbered above every ticket present, and lock it before
/// any other land can read it: written under a temporary name, then renamed into place.
fn take_ticket(
    directory: &Path,
    workspace: &str,
    command: &str,
) -> io::Result<(Ticket, PathBuf, File)> {
    let seq = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(directory.join(SEQ_NAME))?;
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
    file.lock()?;
    file.write_all(&serde_json::to_vec(&ticket).map_err(io::Error::other)?)?;
    let path = directory.join(ticket_name(number));
    fs::rename(&staged, &path)?;
    drop(seq);
    Ok((ticket, path, file))
}

/// The live ticket just ahead of `number` and the live tickets before it, or `None` at the head.
fn ahead_of(directory: &Path, number: u64) -> io::Result<Option<(File, Vec<Ticket>)>> {
    let mut ahead = Vec::new();
    let mut nearest = None;
    for (_, path) in ticket_files(directory)?
        .into_iter()
        .filter(|(other, _)| *other < number)
    {
        let Some(file) = open_ticket(&path)? else {
            continue;
        };
        if live(&file, &path)? {
            ahead.push(read_ticket(&file, &path)?);
            nearest = Some(file);
        }
    }
    Ok(nearest.map(|file| (file, ahead)))
}

/// Wait in `directory`'s queue for this process's turn to land `workspace` with `command`, then
/// take `outside` exclusively too when given: the lock file of lands that do not ask for turns,
/// kept and never removed. `waiting` is told who holds the turn and how many are ahead whenever
/// that changes and every `report_every` while it does not. The turn is held until the [`Turn`]
/// is dropped or this process exits.
pub fn take(
    directory: &Path,
    workspace: &str,
    command: &str,
    outside: Option<&Path>,
    report_every: Duration,
    mut waiting: impl FnMut(&Wait),
) -> io::Result<Turn> {
    fs::create_dir_all(directory)?;
    let (ticket, path, file) = take_ticket(directory, workspace, command)?;
    while let Some((nearest, ahead)) = ahead_of(directory, ticket.number)? {
        let holder = ahead
            .first()
            .cloned()
            .expect("a nearest live ticket is one of those ahead");
        let mut told = Wait::Queued {
            holder,
            ahead: ahead.len(),
        };
        waiting(&told);
        // The wait on the ticket just ahead runs on its own thread, so this one can repeat what
        // it was told at its cadence; the lock is released at once, since only its end matters.
        let (ended, end) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = ended.send(nearest.lock_shared());
        });
        loop {
            match end.recv_timeout(report_every) {
                Ok(result) => {
                    result?;
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let now = observe(directory)?;
                    let mine = now.iter().position(|live| live.number == ticket.number);
                    if let (Some(holder), Some(ahead)) = (now.first(), mine) {
                        told = Wait::Queued {
                            holder: holder.clone(),
                            ahead,
                        };
                    }
                    waiting(&told);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(io::Error::other(
                        "the land turn wait thread ended unanswered",
                    ));
                }
            }
        }
    }
    let outside = match outside {
        None => None,
        Some(lock) => {
            let held = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(lock)?;
            match held.try_lock() {
                Ok(()) => {}
                Err(TryLockError::WouldBlock) => {
                    waiting(&Wait::Outside {
                        lock: lock.to_path_buf(),
                    });
                    held.lock()?;
                }
                Err(TryLockError::Error(error)) => return Err(error),
            }
            Some(held)
        }
    };
    Ok(Turn {
        path,
        _ticket: file,
        _outside: outside,
    })
}
