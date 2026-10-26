//! Land turns: one land of a repository at a time, in the order the lands asked (05_gateway.md
//! "Land turns").
//!
//! A bare `flock(2)` on one lock file serialized lands but never ordered them: every waiter is
//! woken when the holder releases and the first to retry wins, so a land that asks at the moment
//! of a release takes the turn ahead of one that has waited for minutes. Measured 2026-10-21: a
//! 72-commit train queued behind one land, and when that land released, a land that had asked
//! after the train took the turn and held it for 36 minutes.
//!
//! So a land asks the gateway, whose actor keeps every repository's queue: first asked, first
//! served, the head holding the turn. The gateway names the asker by the kernel's identity of its
//! connection's peer and tells it who holds the turn and how many are ahead until the turn is its
//! own. A turn is held by processes, never by descriptors: by its asker until the asker ends it,
//! closes its connection or exits, and by each command the asker binds to it until that command
//! exits. Measured 2026-10-25: tickets held through an inherited `flock(2)` description kept a turn
//! held for as long as any descendant of the land lived, the image cleanup's deletion children
//! among them, and listed tickets whose processes had exited.
//!
//! The head then takes the outside lock, the file lands took before every caller asked for turns.
//! A process outside the queue holding it records nothing, so while the head waits it names the
//! processes with that file open from one descriptor census per report. Measured 2026-10-21: a
//! land waited 1002 s behind an outside holder having said once that "a land that did not ask
//! for a turn" held it, and nothing named the process.

use std::io;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cowshed_os::fork_lock::Fenced;
use cowshed_os::process::Holder;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::net::UnixStream;

use crate::api::dto::LandTurnName;
use crate::held_lock::{HeldLock, Opened};

/// The gateway control operation that asks for a land turn: the connection that asks it is the
/// asker's place in the queue for as long as it is open.
pub const LAND_TURN_OP: &str = "land-turn";
/// The one-shot gateway control operation that lists every repository's land turns.
pub const LAND_TURNS_OP: &str = "land-turns";

/// The directory of every repository's land records: a declared land's `lands.log` and its
/// commands' output (02_workspaces.md "A declared land"), beside the install slot in the host's
/// support directory, which only this user writes. The queues themselves are the gateway's.
pub fn land_turn_root(support_directory: &Path) -> PathBuf {
    support_directory.join("land-turn")
}

/// What a land asks for: its repository's queue, the workspace it lands, and the command its turn
/// runs, which the queue names to whoever waits behind it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnAsk {
    pub repo: LandTurnName,
    pub workspace: String,
    pub command: String,
}

/// One land's place in its repository's queue, as the gateway records it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Ticket {
    /// The order the land asked in: lower holds the turn first.
    pub number: u64,
    /// The process that asked for the turn, as the kernel named its connection's peer.
    pub pid: u32,
    /// The workspace the land lands.
    pub workspace: String,
    /// When the land asked, in milliseconds since the Unix epoch.
    pub asked_unix_ms: u64,
    /// The command the turn runs.
    pub command: String,
    /// The commands bound to the turn that have not exited, by pid: each holds the turn until it
    /// exits, and nothing it starts holds it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bound: Vec<u32>,
}

/// A land that left its queue without ending its turn: its asker closed its connection, was
/// killed or crashed before it recorded how its command ended.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Abandonment {
    /// When the gateway found it gone, in milliseconds since the Unix epoch.
    pub at_unix_ms: u64,
    pub ticket: Ticket,
    /// Whether it held the turn or still waited when it went.
    pub reason: String,
}

/// What an asker sends on its connection once its turn is held.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "step", rename_all = "kebab-case", deny_unknown_fields)]
pub enum TurnStep {
    /// Bind the command `pid` to the turn: it holds the turn until it exits.
    Bind { pid: u32 },
    /// End the asker's hold, recording how its command ended.
    End { outcome: String },
}

/// What the gateway answers on a land-turn connection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "turn", rename_all = "kebab-case", deny_unknown_fields)]
pub enum TurnAnswer {
    /// The asker waits behind `ahead` tickets, the first of which, `holder`, holds the turn: said
    /// when it asks and whenever its queue changes.
    Queued {
        ticket: Ticket,
        holder: Ticket,
        ahead: usize,
    },
    /// The turn is the asker's.
    Held { ticket: Ticket },
    /// The command `pid` holds the turn until it exits.
    Bound { pid: u32 },
    /// The gateway refuses the ask or the step, and why.
    Refused { error: String },
}

/// What a waiting land is told: who holds the turn and how many tickets are ahead of its own.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Wait {
    /// Behind `ahead` tickets, the first of which, `holder`, holds the turn.
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

/// A held turn, the asker's connection to the gateway. The gateway holds the turn for as long as
/// the asker holds this, or any command bound to it ([`Self::bind`]) runs. Ending it records how
/// the land's command ended; dropping it, or this process's exit, ends the asker's hold without an
/// outcome, which the gateway records as an abandonment.
#[derive(Debug)]
pub struct Turn {
    ticket: Ticket,
    connection: Lines<BufReader<Fenced<UnixStream>>>,
    /// The connection's descriptor, open for as long as `connection` is: a child armed by
    /// [`Self::bind_at_exec`] writes its bind to it before its exec.
    socket: RawFd,
    outside: Option<HeldLock>,
}

impl Turn {
    /// The ticket that holds the turn, as the gateway numbered it.
    pub fn ticket(&self) -> &Ticket {
        &self.ticket
    }

    /// Arm `command` to bind itself to the turn as it starts: between its fork and its exec the
    /// child writes the bind step naming its own pid on the turn's connection, which it shares
    /// until its exec closes it (close-on-exec). The gateway reads that step before this process's
    /// end, which follows it on the connection, so a land killed at any point after the fork leaves
    /// the command holding the turn, and one killed before the fork started nothing. A child that
    /// cannot write the step fails before its exec, so its spawn fails and the command never runs.
    /// The gateway's answer is read by [`Self::bound`] once the spawn has returned; a spawn that
    /// fails after the child wrote its step leaves that answer unread, so its caller ends the turn.
    pub fn bind_at_exec(&self, command: &mut std::process::Command) {
        use std::os::unix::process::CommandExt as _;

        let socket = self.socket;
        // SAFETY: the hook runs in the forked child before its exec and calls only getpid and
        // write(2), both async-signal-safe, on a stack buffer; it allocates nothing.
        unsafe {
            command.pre_exec(move || write_bind(socket, std::process::id()));
        }
    }

    /// The gateway's answer to the bind the command `pid`, armed by [`Self::bind_at_exec`] and
    /// just spawned, wrote: `Ok` once the gateway holds the turn for it until it exits. A command
    /// that exited before the gateway could watch it holds nothing.
    pub async fn bound(&mut self, pid: u32) -> io::Result<()> {
        match answer(&mut self.connection).await? {
            TurnAnswer::Bound { pid: bound } if bound == pid => Ok(()),
            TurnAnswer::Refused { error } => Err(io::Error::other(format!(
                "the gateway refused to bind pid {pid} to land turn ticket {}: {error}",
                self.ticket.number
            ))),
            other => Err(unexpected(&other, "a bind")),
        }
    }

    /// End the asker's hold, recording `outcome` (how the command ended). The turn passes on once
    /// every command bound to it has exited too. An end that cannot be sent is said on stderr; the
    /// connection then closes without it and the gateway records the turn abandoned.
    pub fn end(mut self, outcome: String) {
        let sent = line(&TurnStep::End {
            outcome: outcome.clone(),
        })
        .and_then(
            |line| match self.connection.get_mut().get_ref().try_write(&line) {
                Ok(written) if written == line.len() => Ok(()),
                Ok(written) => Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!("{written} of its {} bytes were written", line.len()),
                )),
                Err(error) => Err(error),
            },
        );
        if let Err(error) = sent {
            cowshed_os::eprintln!(
                "cowshed: land turn ticket {}: its end ({outcome}) was not sent to the gateway: \
                 {error}; the gateway records the turn abandoned",
                self.ticket.number,
            );
        }
    }

    /// The outside lock's descriptor, for the command to inherit
    /// ([`crate::held_lock::keep_across_exec`]): the kernel keeps that lock until the command, and
    /// every process it hands the descriptor to, has closed it.
    pub fn outside_descriptor(&self) -> Option<RawFd> {
        self.outside.as_ref().map(HeldLock::descriptor)
    }

    /// Resolves once the gateway has closed the connection that holds the turn, and never before:
    /// the turn is lost with it. Cancel-safe, so a holder waits on it beside its command.
    pub async fn lost(&mut self) -> io::Error {
        match self.connection.next_line().await {
            Ok(None) => io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the gateway closed the connection that holds the turn",
            ),
            Ok(Some(line)) => io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the gateway sent {line:?} to a held turn that asked nothing"),
            ),
            Err(error) => error,
        }
    }
}

/// One JSON line.
fn line(value: &impl Serialize) -> io::Result<Vec<u8>> {
    let mut line = serde_json::to_vec(value).map_err(io::Error::other)?;
    line.push(b'\n');
    Ok(line)
}

async fn send(stream: &mut Fenced<UnixStream>, value: &impl Serialize) -> io::Result<()> {
    stream.write_all(&line(value)?).await
}

/// Write the bind step of `pid`, `{"step":"bind","pid":<pid>}` and a newline, to `socket`, with
/// no allocation on any path: it runs in a forked child before its exec ([`Turn::bind_at_exec`]),
/// and a failed write answers the kernel's errno ([`io::Error::last_os_error`], stored inline),
/// which the spawn reports to the parent.
fn write_bind(socket: RawFd, pid: u32) -> io::Result<()> {
    const PREFIX: &[u8] = br#"{"step":"bind","pid":"#;
    let mut line = [0_u8; PREFIX.len() + 10 + 2];
    line[..PREFIX.len()].copy_from_slice(PREFIX);
    let mut digits = [0_u8; 10];
    let mut count = 0;
    let mut rest = pid;
    loop {
        // The remainder is below ten, so its low byte is the digit.
        digits[count] = b'0' + (rest % 10).to_le_bytes()[0];
        count += 1;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    let mut end = PREFIX.len();
    for digit in digits[..count].iter().rev() {
        line[end] = *digit;
        end += 1;
    }
    line[end..end + 2].copy_from_slice(b"}\n");
    end += 2;
    let mut written = 0;
    while written < end {
        let rest = &line[written..end];
        // SAFETY: `rest` is a live stack buffer of `rest.len()` bytes.
        let wrote = unsafe { libc::write(socket, rest.as_ptr().cast(), rest.len()) };
        match usize::try_from(wrote) {
            Ok(wrote) => written += wrote,
            Err(_) => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
    Ok(())
}

/// The gateway's next answer. A gateway that refuses the request before it reads it as a land turn
/// (an older build, which does not know the operation) answers a control failure,
/// `{"ok":false,…}`, which is returned as that refusal.
async fn answer(connection: &mut Lines<BufReader<Fenced<UnixStream>>>) -> io::Result<TurnAnswer> {
    let Some(line) = connection.next_line().await? else {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "the gateway closed the land turn's connection",
        ));
    };
    decode(&line)
}

fn decode(line: &str) -> io::Result<TurnAnswer> {
    #[derive(Deserialize)]
    struct Failure {
        ok: bool,
        error: Option<String>,
    }
    serde_json::from_str::<TurnAnswer>(line).or_else(|error| {
        match serde_json::from_str::<Failure>(line) {
            Ok(Failure { ok: false, error }) => Ok(TurnAnswer::Refused {
                error: error.unwrap_or_else(|| "it gave no reason".to_owned()),
            }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the gateway answered the land turn with {line:?}: {error}"),
            )),
        }
    })
}

fn unexpected(answer: &TurnAnswer, step: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("the gateway answered {step} with {answer:?}"),
    )
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

/// Ask the gateway at `socket` for `ask`'s turn and wait for it, then take `outside` exclusively
/// too when given: the lock file of lands that do not ask for turns, kept and never removed.
/// `waiting` is told who holds the turn and how many are ahead, or which processes have the
/// outside lock open, whenever that changes and every `report_every` while it does not.
pub async fn take(
    socket: &Path,
    ask: &TurnAsk,
    outside: Option<&Path>,
    report_every: Duration,
    mut waiting: impl FnMut(&Wait),
) -> io::Result<Turn> {
    #[derive(Serialize)]
    struct Asking<'a> {
        op: &'static str,
        #[serde(flatten)]
        ask: &'a TurnAsk,
    }
    let budget = cowshed_os::deadline::start("land-turn connect", cowshed_os::deadline::HANDSHAKE);
    let peer = cowshed_os::deadline::Peer::unix("gateway", socket);
    let stream = cowshed_os::deadline::connect_unix(&budget, &peer, socket)
        .await
        .map_err(|error| {
            let error = io::Error::from(error);
            io::Error::new(
                error.kind(),
                format!("cannot reach the gateway at {}: {error}", socket.display()),
            )
        })?;
    let descriptor = std::os::fd::AsRawFd::as_raw_fd(&stream);
    let mut stream = Fenced::new(stream);
    send(
        &mut stream,
        &Asking {
            op: LAND_TURN_OP,
            ask,
        },
    )
    .await?;
    let mut connection = BufReader::new(stream).lines();
    let mut told: Option<Wait> = None;
    let start = tokio::time::Instant::now() + report_every;
    let mut cadence = tokio::time::interval_at(start, report_every);
    let ticket = loop {
        tokio::select! {
            answered = answer(&mut connection) => match answered? {
                TurnAnswer::Held { ticket } => break ticket,
                TurnAnswer::Queued { holder, ahead, .. } => {
                    let wait = Wait::Queued { holder, ahead };
                    if told.as_ref() != Some(&wait) {
                        waiting(&wait);
                        cadence.reset();
                    }
                    told = Some(wait);
                }
                TurnAnswer::Refused { error } => {
                    return Err(io::Error::other(format!(
                        "the gateway at {} refused the land turn {}: {error}",
                        socket.display(),
                        ask.repo,
                    )));
                }
                other @ TurnAnswer::Bound { .. } => return Err(unexpected(&other, "an ask")),
            },
            _ = cadence.tick() => {
                if let Some(wait) = &told {
                    waiting(wait);
                }
            }
        }
    };
    // The outside lock is the held lock every other holder of it takes, recorded on its inode.
    // Its wait runs on a thread of its own before the first census, so no census delays it.
    let outside = match outside {
        None => None,
        Some(lock) => Some(match Opened::open(lock)?.try_hold()? {
            Ok(held) => held,
            Err(opened) => {
                let me = i32::try_from(std::process::id()).map_err(io::Error::other)?;
                let mut holding = tokio::task::spawn_blocking(move || opened.hold());
                let mut cadence = tokio::time::interval(report_every);
                loop {
                    tokio::select! {
                        held = &mut holding => break held.map_err(io::Error::other)??,
                        _ = cadence.tick() => {
                            let lock = lock.to_path_buf();
                            let wait = tokio::task::spawn_blocking(move || outside_wait(&lock, me))
                                .await
                                .map_err(io::Error::other)?;
                            waiting(&wait);
                        }
                    }
                }
            }
        }),
    };
    Ok(Turn {
        ticket,
        connection,
        socket: descriptor,
        outside,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;

    fn ticket(number: u64) -> Ticket {
        Ticket {
            number,
            pid: 41,
            workspace: "topic".to_owned(),
            asked_unix_ms: 7,
            command: "land".to_owned(),
            bound: vec![42],
        }
    }

    #[test]
    fn an_answer_and_a_control_failure_both_decode_and_anything_else_is_invalid() {
        let held = TurnAnswer::Held { ticket: ticket(3) };
        let line = String::from_utf8(line(&held).unwrap()).unwrap();
        assert_eq!(decode(line.trim_end()).unwrap(), held);
        assert_eq!(
            decode(
                r#"{"ok":false,"code":"invalid-request","error":"unknown gateway control operation"}"#
            )
            .unwrap(),
            TurnAnswer::Refused {
                error: "unknown gateway control operation".to_owned()
            },
        );
        assert_eq!(
            decode(r#"{"ok":true}"#).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn a_step_names_its_kind_and_a_ticket_without_bound_commands_omits_them() {
        assert_eq!(
            serde_json::to_string(&TurnStep::Bind { pid: 9 }).unwrap(),
            r#"{"step":"bind","pid":9}"#
        );
        let mut free = ticket(1);
        free.bound.clear();
        assert!(!serde_json::to_string(&free).unwrap().contains("bound"));
    }

    #[test]
    fn the_bind_a_child_writes_before_its_exec_is_the_bind_step() {
        use std::io::Read as _;
        use std::os::fd::AsRawFd as _;

        for pid in [0, 7, 41_235, u32::MAX] {
            let (ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
            write_bind(theirs.as_raw_fd(), pid).unwrap();
            drop(theirs);
            let mut written = String::new();
            (&ours).read_to_string(&mut written).unwrap();
            let step = written.strip_suffix('\n').expect("one line");
            assert_eq!(
                serde_json::from_str::<TurnStep>(step).unwrap(),
                TurnStep::Bind { pid }
            );
            assert_eq!(
                step,
                serde_json::to_string(&TurnStep::Bind { pid }).unwrap()
            );
        }
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
