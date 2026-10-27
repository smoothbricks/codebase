//! Land turns as the gateway's state (05_gateway.md "Land turns"): every repository's queue, first
//! asked first served, its first ticket holding the turn.
//!
//! A turn is held by processes, never by descriptors. Its asker holds it until it ends its hold,
//! closes its connection or exits; each command the asker binds to it holds it until that command
//! exits; and the turn passes on once the last of them has gone. Nothing a bound command starts
//! holds it. Measured 2026-10-25: tickets held through an inherited `flock(2)` description kept a
//! turn held for as long as any descendant of the land lived, the image cleanup's deletion children
//! among them, and listed tickets whose processes had exited.
//!
//! [`Turns`] is the pure core: it is fed asks, binds, exits and departures with the time they
//! happened and answers what each land is told. [`LandTurns`] is its shell, one task that owns it;
//! [`hold`] maps one land-turn connection, and the exits the kernel reports, into its inputs.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cowshed_core::api::dto::{LandTurnName, LandTurnQueue};
use cowshed_core::land_turn::{Abandonment, Ticket, TurnAnswer, TurnAsk, TurnStep};
use cowshed_core::runtime::job_groups::{Life, Process, ProcessExit};
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncWrite, AsyncWriteExt as _, BufReader};
use tokio::sync::{mpsc, oneshot};

/// How many abandonments the gateway keeps for `land-turns`, newest last.
const ABANDONMENTS_KEPT: usize = 256;

/// One ticket in its queue.
#[derive(Debug)]
struct Entry {
    ticket: Ticket,
    /// The asker still holds its place: its connection is open, its process runs and it has not
    /// ended its hold.
    asker: bool,
    /// The ticket holds the turn: it was its queue's first when the turn last passed on.
    held: bool,
    /// How the asker ended its hold, once it has.
    outcome: Option<String>,
}

impl Entry {
    /// Nothing holds the ticket any more: neither its asker nor a bound command.
    fn done(&self) -> bool {
        !self.asker && self.ticket.bound.is_empty()
    }
}

/// What one land is told: the answer for the ticket `number`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Notice {
    pub number: u64,
    pub answer: TurnAnswer,
}

/// How a ticket left its queue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Ended {
    /// Its asker ended its hold with `outcome`, and every bound command has exited.
    Finished { ticket: Ticket, outcome: String },
    /// Its asker went without ending its hold.
    Abandoned {
        repo: LandTurnName,
        abandonment: Abandonment,
    },
}

/// What one input changed: what its lands are told, and the ticket it ended, if any.
#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct Settled {
    pub notices: Vec<Notice>,
    pub ended: Option<Ended>,
}

/// Why a bind is refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BindRefusal {
    /// No queue holds the ticket: it ended.
    Gone,
    /// The ticket waits: only a held turn runs a command.
    Waiting,
}

impl std::fmt::Display for BindRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Gone => "the ticket has left its queue",
            Self::Waiting => {
                "the ticket still waits for its turn; a command runs only under a held turn"
            }
        })
    }
}

/// Every repository's queue: the pure core of land turns.
#[derive(Debug, Default)]
pub(crate) struct Turns {
    queues: BTreeMap<LandTurnName, Vec<Entry>>,
    abandoned: VecDeque<(LandTurnName, Abandonment)>,
    /// The last ticket number given; the next ask is given the one after it.
    numbered: u64,
}

impl Turns {
    /// `ask` takes its place at the end of its queue, asked by `pid` at `asked_unix_ms`, and is
    /// numbered here, as it arrives: first asked, first served, and no ask that arrived later ever
    /// takes a place ahead of it. Answers its number.
    pub(crate) fn ask(&mut self, ask: TurnAsk, pid: u32, asked_unix_ms: u64) -> (u64, Settled) {
        self.numbered += 1;
        let number = self.numbered;
        let TurnAsk {
            repo,
            workspace,
            command,
        } = ask;
        let queue = self.queues.entry(repo).or_default();
        queue.push(Entry {
            ticket: Ticket {
                number,
                pid,
                workspace,
                asked_unix_ms,
                command,
                bound: Vec::new(),
            },
            asker: true,
            held: false,
            outcome: None,
        });
        (
            number,
            Settled {
                notices: tell(queue),
                ended: None,
            },
        )
    }

    /// Bind the command `pid` to the held ticket `number`: it holds the turn until it exits.
    pub(crate) fn bind(&mut self, number: u64, pid: u32) -> Result<Settled, BindRefusal> {
        let (queue, index) = self.find(number).ok_or(BindRefusal::Gone)?;
        let entry = &mut queue[index];
        if !entry.held {
            return Err(BindRefusal::Waiting);
        }
        if !entry.ticket.bound.contains(&pid) {
            entry.ticket.bound.push(pid);
        }
        Ok(Settled {
            notices: tell(queue),
            ended: None,
        })
    }

    /// The command `pid` bound to ticket `number` exited at `now_unix_ms`.
    pub(crate) fn exited(&mut self, number: u64, pid: u32, now_unix_ms: u64) -> Settled {
        let Some((queue, index)) = self.find(number) else {
            return Settled::default();
        };
        queue[index].ticket.bound.retain(|bound| *bound != pid);
        self.settle(number, now_unix_ms)
    }

    /// The asker of ticket `number` ended its hold at `now_unix_ms`: with `outcome` when it ended
    /// it, without one when its connection closed or its process exited first.
    pub(crate) fn left(
        &mut self,
        number: u64,
        outcome: Option<String>,
        now_unix_ms: u64,
    ) -> Settled {
        let Some((queue, index)) = self.find(number) else {
            return Settled::default();
        };
        let entry = &mut queue[index];
        entry.asker = false;
        if outcome.is_some() {
            entry.outcome = outcome;
        }
        self.settle(number, now_unix_ms)
    }

    /// Every queue with a ticket or an abandonment at or after `abandoned_since_unix_ms`, by name,
    /// each ticket in turn order, the holder first.
    pub(crate) fn queues(&self, abandoned_since_unix_ms: u64) -> Vec<LandTurnQueue> {
        let mut queues: BTreeMap<&LandTurnName, LandTurnQueue> = self
            .queues
            .iter()
            .map(|(repo, entries)| {
                (
                    repo,
                    LandTurnQueue {
                        repo: repo.clone(),
                        tickets: entries.iter().map(|entry| entry.ticket.clone()).collect(),
                        abandoned: Vec::new(),
                    },
                )
            })
            .collect();
        for (repo, abandonment) in self
            .abandoned
            .iter()
            .filter(|(_, abandonment)| abandonment.at_unix_ms >= abandoned_since_unix_ms)
        {
            queues
                .entry(repo)
                .or_insert_with(|| LandTurnQueue {
                    repo: repo.clone(),
                    tickets: Vec::new(),
                    abandoned: Vec::new(),
                })
                .abandoned
                .push(abandonment.clone());
        }
        queues.into_values().collect()
    }

    fn find(&mut self, number: u64) -> Option<(&mut Vec<Entry>, usize)> {
        self.queues.values_mut().find_map(|queue| {
            queue
                .iter()
                .position(|entry| entry.ticket.number == number)
                .map(|index| (queue, index))
        })
    }

    /// Remove ticket `number` once nothing holds it, and tell its queue what changed.
    fn settle(&mut self, number: u64, now_unix_ms: u64) -> Settled {
        let Some(repo) = self
            .queues
            .iter()
            .find(|(_, queue)| queue.iter().any(|entry| entry.ticket.number == number))
            .map(|(repo, _)| repo.clone())
        else {
            return Settled::default();
        };
        let queue = self
            .queues
            .get_mut(&repo)
            .expect("the queue was just found");
        let index = queue
            .iter()
            .position(|entry| entry.ticket.number == number)
            .expect("the ticket was just found");
        let ended = if queue[index].done() {
            let Entry {
                ticket,
                held,
                outcome,
                ..
            } = queue.remove(index);
            Some(match outcome {
                Some(outcome) => Ended::Finished { ticket, outcome },
                None => {
                    let abandonment = Abandonment {
                        at_unix_ms: now_unix_ms,
                        ticket,
                        reason: if held {
                            "it held the turn, and its asker went without ending it".to_owned()
                        } else {
                            "it left the queue before its turn".to_owned()
                        },
                    };
                    if self.abandoned.len() == ABANDONMENTS_KEPT {
                        self.abandoned.pop_front();
                    }
                    self.abandoned
                        .push_back((repo.clone(), abandonment.clone()));
                    Ended::Abandoned {
                        repo: repo.clone(),
                        abandonment,
                    }
                }
            })
        } else {
            None
        };
        let notices = tell(queue);
        if queue.is_empty() {
            self.queues.remove(&repo);
        }
        Settled { notices, ended }
    }
}

/// What every land in `queue` is told now: its first ticket that it holds the turn, once, when it
/// takes it; every other who holds the turn and how many are ahead of it.
fn tell(queue: &mut [Entry]) -> Vec<Notice> {
    let Some((head, waiters)) = queue.split_first_mut() else {
        return Vec::new();
    };
    let mut notices = Vec::with_capacity(waiters.len() + 1);
    if !head.held {
        head.held = true;
        notices.push(Notice {
            number: head.ticket.number,
            answer: TurnAnswer::Held {
                ticket: head.ticket.clone(),
            },
        });
    }
    for (ahead, waiter) in waiters.iter().enumerate() {
        notices.push(Notice {
            number: waiter.ticket.number,
            answer: TurnAnswer::Queued {
                ticket: waiter.ticket.clone(),
                holder: head.ticket.clone(),
                ahead: ahead + 1,
            },
        });
    }
    notices
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

enum Message {
    Ask {
        /// The connection that asks, whose departure names it ([`Message::Left`]).
        connection: u64,
        ask: TurnAsk,
        pid: u32,
        answers: mpsc::UnboundedSender<TurnAnswer>,
        /// Told the ticket's number once the actor has given it.
        number: oneshot::Sender<u64>,
    },
    Bind {
        number: u64,
        pid: u32,
        done: oneshot::Sender<Result<(), BindRefusal>>,
    },
    Exited {
        number: u64,
        pid: u32,
    },
    Left {
        connection: u64,
        outcome: Option<String>,
    },
    Queues {
        abandoned_since_unix_ms: u64,
        reply: oneshot::Sender<Vec<LandTurnQueue>>,
    },
}

/// The land turns' shell: one task owns [`Turns`], fed through this handle.
#[derive(Clone, Debug)]
pub(crate) struct LandTurns {
    messages: mpsc::UnboundedSender<Message>,
    /// Names each land-turn connection; the actor alone gives ticket numbers.
    connections: Arc<AtomicU64>,
}

impl LandTurns {
    pub(crate) fn start() -> Self {
        let (messages, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run(receiver));
        Self {
            messages,
            connections: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Every queue with a ticket or a recent abandonment; `None` once the task has stopped.
    pub(crate) async fn queues(&self, abandoned_since_unix_ms: u64) -> Option<Vec<LandTurnQueue>> {
        let (reply, answer) = oneshot::channel();
        self.messages
            .send(Message::Queues {
                abandoned_since_unix_ms,
                reply,
            })
            .ok()?;
        answer.await.ok()
    }

    fn send(&self, message: Message) {
        // A send fails only once the task has stopped, with the gateway: nothing is held then.
        let _ = self.messages.send(message);
    }
}

async fn run(mut receiver: mpsc::UnboundedReceiver<Message>) {
    let mut turns = Turns::default();
    let mut answers: HashMap<u64, mpsc::UnboundedSender<TurnAnswer>> = HashMap::new();
    // Each asking connection's ticket number, which its departure names.
    let mut numbers: HashMap<u64, u64> = HashMap::new();
    while let Some(message) = receiver.recv().await {
        let settled = match message {
            Message::Ask {
                connection,
                ask,
                pid,
                answers: sender,
                number,
            } => {
                let repo = ask.repo.clone();
                let asked = format!("{} (pid {pid}) for `{}`", ask.workspace, ask.command);
                let (given, settled) = turns.ask(ask, pid, now_unix_ms());
                cowshed_os::eprintln!("cowshed: land turn {repo}: ticket {given} asked by {asked}");
                // A connection that has gone hears nothing; its departure is on its way.
                let _ = number.send(given);
                numbers.insert(connection, given);
                answers.insert(given, sender);
                settled
            }
            Message::Bind { number, pid, done } => {
                let bound = turns.bind(number, pid);
                let reply = bound.as_ref().map(|_| ()).map_err(Clone::clone);
                let _ = done.send(reply);
                match bound {
                    Ok(settled) => {
                        cowshed_os::eprintln!(
                            "cowshed: land turn ticket {number}: command pid {pid} holds it until it exits"
                        );
                        settled
                    }
                    Err(_) => Settled::default(),
                }
            }
            Message::Exited { number, pid } => turns.exited(number, pid, now_unix_ms()),
            Message::Left {
                connection,
                outcome,
            } => match numbers.remove(&connection) {
                Some(number) => turns.left(number, outcome, now_unix_ms()),
                None => Settled::default(),
            },
            Message::Queues {
                abandoned_since_unix_ms,
                reply,
            } => {
                let _ = reply.send(turns.queues(abandoned_since_unix_ms));
                Settled::default()
            }
        };
        for Notice { number, answer } in settled.notices {
            if let TurnAnswer::Held { ticket } = &answer {
                cowshed_os::eprintln!(
                    "cowshed: land turn ticket {number}: {} (pid {}) holds the turn after {} ms",
                    ticket.workspace,
                    ticket.pid,
                    now_unix_ms().saturating_sub(ticket.asked_unix_ms),
                );
            }
            // A land whose connection has gone hears nothing; its departure is on its way.
            if let Some(sender) = answers.get(&number) {
                let _ = sender.send(answer);
            }
        }
        match settled.ended {
            None => {}
            Some(Ended::Finished { ticket, outcome }) => {
                answers.remove(&ticket.number);
                cowshed_os::eprintln!(
                    "cowshed: land turn ticket {}: {} (pid {}) finished: {outcome}",
                    ticket.number,
                    ticket.workspace,
                    ticket.pid,
                );
            }
            Some(Ended::Abandoned { repo, abandonment }) => {
                answers.remove(&abandonment.ticket.number);
                cowshed_os::eprintln!(
                    "cowshed: land turn {repo}: ticket {} of {} (pid {}) abandoned: {}",
                    abandonment.ticket.number,
                    abandonment.ticket.workspace,
                    abandonment.ticket.pid,
                    abandonment.reason,
                );
            }
        }
    }
}

/// The asker's place in its queue for as long as its connection is served: dropped on any end of
/// that service, it ends the asker's hold without an outcome unless one was sent.
struct Place {
    turns: LandTurns,
    connection: u64,
    outcome: Option<String>,
}

impl Drop for Place {
    fn drop(&mut self) {
        self.turns.send(Message::Left {
            connection: self.connection,
            outcome: self.outcome.take(),
        });
    }
}

/// Serve one land-turn connection whose peer the kernel named `asker`: queue `ask`, tell the asker
/// every answer for its ticket, bind the commands it names once its turn is held, and end its hold
/// when it ends it, closes the connection or exits.
pub(crate) async fn hold<R, W>(
    reader: R,
    mut writer: W,
    asker: Option<Process>,
    ask: TurnAsk,
    turns: LandTurns,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let Some(asker) = asker else {
        let _ = write(
            &mut writer,
            &TurnAnswer::Refused {
                error: "a land turn is asked over the gateway's Unix control socket, whose peer \
                        the kernel names"
                    .to_owned(),
            },
        )
        .await;
        return;
    };
    let exit = match asker.exit() {
        Ok(exit) => exit,
        Err(error) => {
            let _ = write(
                &mut writer,
                &TurnAnswer::Refused {
                    error: format!("cannot watch the asking process's exit: {error}"),
                },
            )
            .await;
            return;
        }
    };
    let connection = turns.connections.fetch_add(1, Ordering::Relaxed);
    let (sender, mut answers) = mpsc::unbounded_channel();
    let (numbered, number) = oneshot::channel();
    turns.send(Message::Ask {
        connection,
        ask,
        pid: asker.pid().unsigned_abs(),
        answers: sender,
        number: numbered,
    });
    // Placed before its number is awaited, so a hold that ends meanwhile still leaves its queue.
    let mut place = Place {
        turns: turns.clone(),
        connection,
        outcome: None,
    };
    let Ok(number) = number.await else { return };
    let asker_gone = async {
        if let Err(error) = exit.observed().await {
            cowshed_os::eprintln!(
                "cowshed: land turn ticket {number}: its asker's exit watch failed ({error}); its \
                 connection alone holds its place"
            );
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(asker_gone);
    let mut steps = BufReader::new(reader).lines();
    let mut held = false;
    loop {
        // Biased, in this order: an asker that sends its end and exits leaves both ready, and its
        // end, already in the socket's buffer, is read before its exit, so the turn records how
        // the command ended rather than an abandonment. An answer that cannot be written means
        // the asker is gone, not that its steps are: what it sent before it went is read below.
        tokio::select! {
            biased;
            answer = answers.recv() => {
                let Some(answer) = answer else { return };
                held |= matches!(answer, TurnAnswer::Held { .. });
                if write(&mut writer, &answer).await.is_err() {
                    break;
                }
            }
            step = steps.next_line() => {
                // EOF, or a broken connection: the asker is gone, and so is its hold.
                let Ok(Some(step)) = step else { return };
                let Some(answer) = stepped(&turns, number, held, &step, &mut place).await else {
                    return;
                };
                if write(&mut writer, &answer).await.is_err() {
                    break;
                }
            }
            () = &mut asker_gone => break,
        }
    }
    // The asker has exited or stopped reading, but a command it forked may still be between its
    // fork and its exec, its bind on the way on this connection, which it shares until its exec
    // closes it. EOF, once every copy is closed, is the proof that none is: each bind before it is
    // read and bound, so no command runs outside the turn (05_gateway.md "Land turns").
    while let Ok(Some(step)) = steps.next_line().await {
        let Some(answer) = stepped(&turns, number, held, &step, &mut place).await else {
            return;
        };
        // Nobody reads it: the asker is gone, and a command reads no answer after its exec. The
        // bind holds whether or not the answer is written.
        write(&mut writer, &answer).await.ok();
    }
}

/// The answer to one step line of ticket `number`, or `None` once it ends the asker's hold, its
/// outcome recorded in `place`. A bind is refused while the ticket waits.
async fn stepped(
    turns: &LandTurns,
    number: u64,
    held: bool,
    step: &str,
    place: &mut Place,
) -> Option<TurnAnswer> {
    Some(match serde_json::from_str::<TurnStep>(step) {
        Ok(TurnStep::End { outcome }) => {
            place.outcome = Some(outcome);
            return None;
        }
        Ok(TurnStep::Bind { pid }) if held => bind(turns, number, pid).await,
        Ok(TurnStep::Bind { .. }) => TurnAnswer::Refused {
            error: BindRefusal::Waiting.to_string(),
        },
        Err(error) => TurnAnswer::Refused {
            error: format!("a land turn step is a bind or an end: {error}"),
        },
    })
}

/// Bind the command `pid` to ticket `number`, its exit watched from before the bind and across an
/// exec ([`Process::watch_life`]: a command binds itself between its fork and its exec), so no exit
/// after it is missed. A command that has exited already holds nothing and is answered bound.
async fn bind(turns: &LandTurns, number: u64, pid: u32) -> TurnAnswer {
    let refused = |error: String| TurnAnswer::Refused { error };
    let (life, exit) = match i32::try_from(pid)
        .map_err(std::io::Error::other)
        .and_then(Process::watch_life)
    {
        Ok(Some(watched)) => watched,
        Ok(None) => return TurnAnswer::Bound { pid },
        Err(error) => return refused(format!("cannot watch the exit of process {pid}: {error}")),
    };
    let (done, bound) = oneshot::channel();
    turns.send(Message::Bind { number, pid, done });
    match bound.await {
        Ok(Ok(())) => {
            let turns = turns.clone();
            tokio::spawn(async move {
                exited(&life, exit, number, pid).await;
                turns.send(Message::Exited { number, pid });
            });
            TurnAnswer::Bound { pid }
        }
        Ok(Err(refusal)) => refused(refusal.to_string()),
        Err(_) => refused("the gateway's land turns have stopped".to_owned()),
    }
}

/// Resolves once the bound command `pid` has exited. A watch that fails is armed again once, on the
/// life it bound ([`Life::exit`]), so a later process given its pid is never taken for it; a second
/// failure never resolves, so the command holds the turn for as long as the gateway runs, and says
/// so on stderr: a command that may still run is never taken for exited (05_gateway.md "Land
/// turns").
async fn exited(life: &Life, exit: ProcessExit, number: u64, pid: u32) {
    let Err(error) = exit.observed().await else {
        return;
    };
    cowshed_os::eprintln!(
        "cowshed: land turn ticket {number}: the exit watch of its command pid {pid} failed \
         ({error}); watching it again"
    );
    let again = match life.exit() {
        Ok(exit) => exit.observed().await.map(|_| ()),
        Err(error) => Err(error),
    };
    if let Err(error) = again {
        cowshed_os::eprintln!(
            "cowshed: land turn ticket {number}: its command pid {pid} cannot be watched again \
             ({error}); it holds the turn until the gateway restarts"
        );
        std::future::pending::<()>().await;
    }
}

async fn write<W: AsyncWrite + Unpin>(writer: &mut W, answer: &TurnAnswer) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(answer).map_err(std::io::Error::other)?;
    line.push(b'\n');
    writer.write_all(&line).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(repo: &str, workspace: &str) -> TurnAsk {
        TurnAsk {
            repo: LandTurnName::new(repo).unwrap(),
            workspace: workspace.to_owned(),
            command: format!("land {workspace}"),
        }
    }

    /// Who each notice tells what: `(number, "held")` or `(number, "queued <holder> <ahead>")`.
    fn told(settled: &Settled) -> Vec<(u64, String)> {
        settled
            .notices
            .iter()
            .map(|Notice { number, answer }| {
                (
                    *number,
                    match answer {
                        TurnAnswer::Held { .. } => "held".to_owned(),
                        TurnAnswer::Queued { holder, ahead, .. } => {
                            format!("queued {} {ahead}", holder.number)
                        }
                        other => format!("{other:?}"),
                    },
                )
            })
            .collect()
    }

    fn holders(turns: &Turns, repo: &str) -> Vec<u64> {
        turns
            .queues(u64::MAX)
            .into_iter()
            .filter(|queue| queue.repo.as_str() == repo)
            .flat_map(|queue| queue.tickets.into_iter().map(|ticket| ticket.number))
            .collect()
    }

    /// The answers a gone asker cannot be written, never fail to be: a writer that refuses every
    /// write ends nothing the asker sent before it went.
    struct Gone;

    impl AsyncWrite for Gone {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// A land whose command fails to start sends its end and exits, and the gateway may answer
    /// its turn only after: the answer it cannot write ends nothing, the end already sent is read,
    /// and the ticket finishes with its outcome instead of being recorded abandoned
    /// (05_gateway.md, "A ticket's life is said").
    #[tokio::test]
    async fn an_end_sent_before_an_unwritable_answer_finishes_the_ticket() {
        let (_asker, gateway) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let asker = Process::of_socket_peer(&gateway).expect("the peer is this process");
        let turns = LandTurns::start();
        let steps: &[u8] = b"{\"step\":\"end\",\"outcome\":\"did not start: missing\"}\n";
        hold(
            steps,
            Gone,
            Some(asker),
            ask("repo", "missing"),
            turns.clone(),
        )
        .await;
        let queues = turns.queues(0).await.expect("the land turns run");
        assert!(
            queues.is_empty(),
            "the ticket finished, neither held nor abandoned: {queues:?}"
        );
    }

    #[test]
    fn the_first_ask_holds_the_turn_and_each_later_one_is_told_who_holds_it() {
        let mut turns = Turns::default();
        assert_eq!(
            told(&turns.ask(ask("repo", "a"), 10, 0).1),
            [(1, "held".to_owned())]
        );
        assert_eq!(
            told(&turns.ask(ask("repo", "b"), 20, 1).1),
            [(2, "queued 1 1".to_owned())]
        );
        assert_eq!(
            told(&turns.ask(ask("repo", "c"), 30, 2).1),
            [(2, "queued 1 1".to_owned()), (3, "queued 1 2".to_owned())]
        );
        // Another repository's queue is its own.
        assert_eq!(
            told(&turns.ask(ask("other", "d"), 40, 3).1),
            [(4, "held".to_owned())]
        );
    }

    #[test]
    fn numbers_are_given_in_the_order_asks_arrive_and_each_waits_behind_every_earlier_one() {
        let mut turns = Turns::default();
        let numbers: Vec<u64> = [("repo", "a"), ("other", "b"), ("repo", "c"), ("repo", "d")]
            .into_iter()
            .map(|(repo, workspace)| turns.ask(ask(repo, workspace), 10, 0).0)
            .collect();
        assert_eq!(numbers, [1, 2, 3, 4]);
        assert_eq!(holders(&turns, "repo"), [1, 3, 4]);
        assert_eq!(holders(&turns, "other"), [2]);
    }

    #[test]
    fn a_bound_command_holds_the_turn_past_its_asker_and_its_exit_passes_it_on() {
        let mut turns = Turns::default();
        let (first, _) = turns.ask(ask("repo", "a"), 10, 0);
        let (second, _) = turns.ask(ask("repo", "b"), 20, 1);
        turns.bind(first, 11).unwrap();
        // The asker is killed: its command still runs, so the turn stays its own.
        let left = turns.left(first, None, 5);
        assert_eq!(left.ended, None);
        assert_eq!(holders(&turns, "repo"), [first, second]);
        let exited = turns.exited(first, 11, 9);
        assert_eq!(told(&exited), [(second, "held".to_owned())]);
        assert!(matches!(
            &exited.ended,
            Some(Ended::Abandoned { abandonment: Abandonment { at_unix_ms: 9, ticket, .. }, .. })
                if ticket.number == first
        ));
    }

    #[test]
    fn an_ended_asker_whose_command_has_exited_finishes_with_its_outcome() {
        let mut turns = Turns::default();
        let (first, _) = turns.ask(ask("repo", "a"), 10, 0);
        turns.bind(first, 11).unwrap();
        // The end may reach the gateway before the command's exit does, or after it.
        assert_eq!(
            turns.left(first, Some("exited 0".to_owned()), 4).ended,
            None
        );
        assert_eq!(
            turns.exited(first, 11, 5).ended,
            Some(Ended::Finished {
                ticket: Ticket {
                    number: first,
                    pid: 10,
                    workspace: "a".to_owned(),
                    asked_unix_ms: 0,
                    command: "land a".to_owned(),
                    bound: Vec::new(),
                },
                outcome: "exited 0".to_owned(),
            })
        );
        assert!(
            turns.queues(0).is_empty(),
            "a finished ticket is no abandonment"
        );
    }

    #[test]
    fn a_waiter_that_leaves_blocks_nobody_and_is_recorded_abandoned() {
        let mut turns = Turns::default();
        let (first, _) = turns.ask(ask("repo", "a"), 10, 0);
        let (second, _) = turns.ask(ask("repo", "b"), 20, 1);
        let (third, _) = turns.ask(ask("repo", "c"), 30, 2);
        let left = turns.left(second, None, 7);
        assert_eq!(told(&left), [(third, "queued 1 1".to_owned())]);
        assert!(matches!(
            &left.ended,
            Some(Ended::Abandoned { abandonment, .. }) if abandonment.ticket.number == second
        ));
        assert_eq!(
            told(&turns.left(first, Some("exited 0".to_owned()), 8)),
            [(third, "held".to_owned())]
        );
        let queues = turns.queues(7);
        assert_eq!(queues.len(), 1);
        assert_eq!(
            queues[0]
                .abandoned
                .iter()
                .map(|gone| (gone.ticket.number, gone.reason.as_str()))
                .collect::<Vec<_>>(),
            [(second, "it left the queue before its turn")]
        );
        assert!(
            turns.queues(8)[0].abandoned.is_empty(),
            "an older abandonment is not listed"
        );
    }

    #[test]
    fn a_waiting_ticket_binds_nothing_and_a_gone_one_is_named_gone() {
        let mut turns = Turns::default();
        let (first, _) = turns.ask(ask("repo", "a"), 10, 0);
        let (second, _) = turns.ask(ask("repo", "b"), 20, 1);
        assert_eq!(turns.bind(second, 21), Err(BindRefusal::Waiting));
        turns.left(first, Some("exited 0".to_owned()), 2);
        assert_eq!(turns.bind(first, 11), Err(BindRefusal::Gone));
        assert!(turns.bind(second, 21).is_ok());
    }

    #[test]
    fn the_abandonments_kept_are_the_newest() {
        let mut turns = Turns::default();
        let total = u64::try_from(ABANDONMENTS_KEPT).unwrap() + 3;
        for at in 1..=total {
            let (number, _) = turns.ask(ask("repo", "a"), 10, at);
            turns.left(number, None, at);
        }
        let queues = turns.queues(0);
        let kept: Vec<u64> = queues[0]
            .abandoned
            .iter()
            .map(|gone| gone.ticket.number)
            .collect();
        assert_eq!(kept.len(), ABANDONMENTS_KEPT);
        assert_eq!(kept.first(), Some(&4));
        assert_eq!(kept.last(), Some(&total));
    }
}
