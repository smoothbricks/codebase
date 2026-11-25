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
//! An install's read of the turns, asked with the fence, fences every queue when it finds no
//! holder, the read and the fence one step. An install that finds tickets may wait for them
//! instead: it takes its place after every ticket queued when it asked, and is fenced in the step
//! that ends the last of them. An ask that meets a waiting install or a fence is answered
//! `fenced` and parked, never numbered, on the connection it asked on; once no install waits and
//! no fence holds, the parked asks are numbered in the order they arrived (05_gateway.md "The read
//! fences the turns"). One ask is never parked: an ask made inside a held turn, by its asker
//! or by a command bound to it, as a land asks its ref turn inside its land turn. The install
//! waits for that turn, which ends only once the ask made inside it is served. Recorded
//! 2026-11-16: such an ask was parked behind the install of the land before, and the two waited
//! for each other for 24 minutes, until the land was ended by hand. Recorded 2026-10-28
//! 13:52:39Z: an install read no holder, a land asked its
//! fences the turns"). Recorded 2026-10-28 13:52:39Z: an install read no holder, a land asked its
//! turn and bound its gate, and the install's restart closed the turn's connection, so the land
//! sent its gate SIGTERM. Recorded 2026-11-10: an install that deferred whenever it found a turn
//! held or queued deferred three times, each rerun losing the gap to the next land's ask.
//!
//! [`Turns`] is the pure core: it is fed asks, binds, exits, departures, fences and waits with the
//! time they happened and answers what each land and install is told. [`LandTurns`] is its shell,
//! one task that owns it; [`hold`] maps one land-turn connection, and the exits the kernel reports,
//! into its inputs.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cowshed_core::api::dto::{
    LandTurnName, LandTurnQueue, ParkedLandTurn, WaitingInstall, WithdrawnInstall,
};
use cowshed_core::land_turn::{Abandonment, Ticket, TurnAnswer, TurnAsk, TurnStep, format_bound};
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
    /// When the ticket took the turn, in milliseconds since the Unix epoch.
    held_unix_ms: Option<u64>,
    /// Whether the ticket was asked with `"class":"bounded"`.
    bounded: bool,
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
    Finished {
        ticket: Ticket,
        outcome: String,
        bounded: bool,
        held_ms: Option<u64>,
    },
    /// Its asker went without ending its hold.
    Abandoned {
        repo: LandTurnName,
        abandonment: Abandonment,
        bounded: bool,
        held_ms: Option<u64>,
    },
}

/// What one input changed: what its lands are told, the ticket it ended, if any, the waiting
/// installs it fenced and the parked asks it numbered.
#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct Settled {
    pub notices: Vec<Notice>,
    pub ended: Option<Ended>,
    /// The waiting installs fenced in this step, by their keys: it ended the last ticket.
    pub fenced: Vec<u64>,
    /// The parked asks numbered in this step, `(key, number)` in arrival order: no install waits
    /// and no fence holds any more.
    pub numbered: Vec<(u64, u64)>,
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

/// An ask from outside every held turn that met an install waiting for the turns or fencing them:
/// parked, never numbered, until no install waits and no fence holds.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Parked;

/// An ask parked behind an install, the key the actor names its connection by, its asker and when
/// it arrived.
#[derive(Debug)]
struct ParkedAsk {
    key: u64,
    ask: TurnAsk,
    pid: u32,
    asked_unix_ms: u64,
}

/// One install currently waiting for the queues to empty, keyed by the actor's id, carrying its
/// peer pid and when it began waiting for `gateway status` and withdrawal reports.
#[derive(Clone, Copy, Debug)]
struct WaitingEntry {
    install: u64,
    pid: Option<u32>,
    waiting_unix_ms: u64,
}

/// Every repository's queue: the pure core of land turns.
#[derive(Debug, Default)]
pub(crate) struct Turns {
    queues: BTreeMap<LandTurnName, Vec<Entry>>,
    abandoned: VecDeque<(LandTurnName, Abandonment)>,
    /// The last ticket number given; the next ask is given the one after it.
    numbered: u64,
    /// How many installs fence every queue now: an ask that meets one is parked.
    fences: u64,
    /// The installs waiting for every queue to empty, by the key the actor gave each: an ask from
    /// outside every held turn that meets one is parked, so no such ask lengthens its wait.
    waiting: Vec<WaitingEntry>,
    /// The waiting installs that were fenced and hold that fence until they leave, by key.
    fenced_installs: Vec<u64>,
    /// The asks parked behind a waiting install or a fence, in arrival order.
    parked: VecDeque<ParkedAsk>,
}

impl Turns {
    /// `ask`, whose connection the actor keys `key`, takes its place at the end of its queue, asked
    /// by `pid` at `asked_unix_ms`, and is numbered here, as it arrives: first asked, first served,
    /// and no ask that arrived later ever takes a place ahead of it. Answers its number, or
    /// [`Parked`] when an install waits for the queues or fences them and `pid` asks from outside
    /// every held turn ([`Self::inside`]): then no queue changes, and the ask is numbered once no
    /// install waits and no fence holds ([`Settled::numbered`]).
    pub(crate) fn ask(
        &mut self,
        key: u64,
        ask: TurnAsk,
        pid: u32,
        asked_unix_ms: u64,
    ) -> Result<(u64, Settled), Parked> {
        let should_park = !self.inside(pid)
            && (self.fences > 0
                || (!self.waiting.is_empty() && (!ask.is_bounded() || !self.has_unbounded())));
        if should_park {
            self.parked.push_back(ParkedAsk {
                key,
                ask,
                pid,
                asked_unix_ms,
            });
            return Err(Parked);
        }
        let (number, notices) = self.number(ask, pid, asked_unix_ms, asked_unix_ms);
        Ok((
            number,
            Settled {
                notices,
                ..Settled::default()
            },
        ))
    }

    /// Whether any queue currently holds a non-bounded ticket: while one remains and no fence
    /// holds, a bounded ask from outside may still enter behind it while an install waits
    /// (05_gateway.md "The read fences the turns").
    fn has_unbounded(&self) -> bool {
        self.queues.values().flatten().any(|entry| !entry.bounded)
    }

    /// `pid` asks from inside a held turn: it is a command bound to a turn, or the asker of a
    /// ticket that holds its turn and has not ended its hold. A waiting install waits for that
    /// turn, and the turn ends only once what its holder asks inside it is served (a land takes
    /// its ref turn inside its land turn), so such an ask is never parked behind the install. A
    /// process a bound command starts holds no turn and asks from outside. The asker's exit and
    /// each bound command's are watched, so a pid named here is that process's own; only a bound
    /// command whose watch failed twice may lend its pid to a later process, whose ask is then
    /// numbered ahead of the install instead of behind it.
    fn inside(&self, pid: u32) -> bool {
        self.queues.values().flatten().any(|entry| {
            entry.ticket.bound.contains(&pid)
                || (entry.held && entry.asker && entry.ticket.pid == pid)
        })
    }
    /// Number `ask` one past the last number given and put it at the end of its queue: its number
    /// and what its queue is told.
    fn number(
        &mut self,
        ask: TurnAsk,
        pid: u32,
        asked_unix_ms: u64,
        now_unix_ms: u64,
    ) -> (u64, Vec<Notice>) {
        self.numbered += 1;
        let number = self.numbered;
        let bounded = ask.is_bounded();
        let TurnAsk {
            repo,
            workspace,
            command,
            class: _,
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
                fenced: false,
            },
            asker: true,
            held: false,
            held_unix_ms: None,
            bounded,
            outcome: None,
        });
        (number, tell(queue, now_unix_ms))
    }

    /// An install's read of the turns asked with the fence: with no ticket in any queue, so no
    /// holder, every queue is fenced until [`Self::unfence`]; with one, nothing is set, since the
    /// install refuses. A parked ask is no ticket. Answers whether it fenced.
    pub(crate) fn fence(&mut self) -> bool {
        let free = self.queues.is_empty();
        if free {
            self.fences += 1;
        }
        free
    }

    /// An install, keyed `install`, waits for the turns: with no ticket in any queue it is fenced
    /// now, as [`Self::fence`]; otherwise it waits, every ask meanwhile is parked, and it is fenced
    /// in the step that ends the last ticket ([`Settled::fenced`]). Answers whether it fenced now.
    pub(crate) fn wait_fence(
        &mut self,
        install: u64,
        pid: Option<u32>,
        waiting_unix_ms: u64,
    ) -> bool {
        if self.fence() {
            self.fenced_installs.push(install);
            return true;
        }
        self.waiting.push(WaitingEntry {
            install,
            pid,
            waiting_unix_ms,
        });
        false
    }

    /// The install `install` that waited for the turns leaves: it no longer waits, or the fence
    /// its wait was given is lifted; once no install waits and no fence holds, the parked asks are
    /// numbered. An install that has already left changes nothing: leaving is ensuring it is gone.
    pub(crate) fn wait_left(&mut self, install: u64, now_unix_ms: u64) -> Settled {
        if let Some(index) = self
            .waiting
            .iter()
            .position(|waiting| waiting.install == install)
        {
            self.waiting.remove(index);
        } else if let Some(index) = self
            .fenced_installs
            .iter()
            .position(|fenced| *fenced == install)
        {
            self.fenced_installs.remove(index);
            self.fences = self.fences.saturating_sub(1);
        } else {
            return Settled::default();
        }
        self.release(now_unix_ms)
    }

    /// Withdraw every install currently waiting for the queues to empty, never touching an install
    /// that already holds a fence (`TWaitLeave` per waiting install). Once the last waiting install
    /// leaves and no fence holds, every parked ask is numbered in arrival order.
    pub(crate) fn withdraw_waiting(
        &mut self,
        now_unix_ms: u64,
    ) -> (Vec<(u64, WithdrawnInstall)>, Settled) {
        let entries: Vec<WaitingEntry> = self.waiting.clone();
        let mut withdrawn = Vec::with_capacity(entries.len());
        let mut settled = Settled::default();
        for entry in entries {
            let step = self.wait_left(entry.install, now_unix_ms);
            settled.notices.extend(step.notices);
            settled.numbered.extend(step.numbered);
            withdrawn.push((
                entry.install,
                WithdrawnInstall {
                    pid: entry.pid,
                    waiting_unix_ms: entry.waiting_unix_ms,
                    waited_ms: now_unix_ms.saturating_sub(entry.waiting_unix_ms),
                },
            ));
        }
        (withdrawn, settled)
    }

    /// Every install currently waiting for the queues to empty, in wait order.
    pub(crate) fn waiting_installs(&self) -> Vec<WaitingInstall> {
        self.waiting
            .iter()
            .map(|entry| WaitingInstall {
                pid: entry.pid,
                waiting_unix_ms: entry.waiting_unix_ms,
            })
            .collect()
    }

    /// Every ask currently parked behind a waiting install or a fence, in arrival order.
    pub(crate) fn parked_asks(&self) -> Vec<ParkedLandTurn> {
        self.parked
            .iter()
            .map(|entry| ParkedLandTurn {
                repo: entry.ask.repo.clone(),
                workspace: entry.ask.workspace.clone(),
                pid: entry.pid,
                asked_unix_ms: entry.asked_unix_ms,
                command: entry.ask.command.clone(),
            })
            .collect()
    }

    /// A fenced read's connection closed: its fence is lifted, and once no install waits and no
    /// fence holds, the parked asks are numbered.
    pub(crate) fn unfence(&mut self, now_unix_ms: u64) -> Settled {
        self.fences = self.fences.saturating_sub(1);
        self.release(now_unix_ms)
    }

    /// The parked asker keyed `key` left: it is never numbered.
    pub(crate) fn unpark(&mut self, key: u64) {
        self.parked.retain(|parked| parked.key != key);
    }

    /// Number every parked ask in arrival order once no install waits and no fence holds.
    fn release(&mut self, now_unix_ms: u64) -> Settled {
        let mut settled = Settled::default();
        if self.fences > 0 || !self.waiting.is_empty() {
            return settled;
        }
        while let Some(ParkedAsk {
            key,
            ask,
            pid,
            asked_unix_ms,
        }) = self.parked.pop_front()
        {
            let (number, notices) = self.number(ask, pid, asked_unix_ms, now_unix_ms);
            settled.numbered.push((key, number));
            settled.notices.extend(notices);
        }
        settled
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
            notices: tell(queue, 0),
            ..Settled::default()
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
                held_unix_ms,
                bounded,
                outcome,
                ..
            } = queue.remove(index);
            let held_ms = held_unix_ms.map(|since| now_unix_ms.saturating_sub(since));
            Some(match outcome {
                Some(outcome) => Ended::Finished {
                    ticket,
                    outcome,
                    bounded,
                    held_ms,
                },
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
                        bounded,
                        held_ms,
                    }
                }
            })
        } else {
            None
        };
        let notices = tell(queue, now_unix_ms);
        if queue.is_empty() {
            self.queues.remove(&repo);
        }
        let mut settled = Settled {
            notices,
            ended,
            ..Settled::default()
        };
        // The step that ends the last ticket fences every waiting install.
        if self.queues.is_empty() && !self.waiting.is_empty() {
            self.fences += u64::try_from(self.waiting.len()).unwrap_or(u64::MAX);
            let keys: Vec<u64> = std::mem::take(&mut self.waiting)
                .into_iter()
                .map(|entry| entry.install)
                .collect();
            self.fenced_installs.extend_from_slice(&keys);
            settled.fenced = keys;
        }
        settled
    }
}

/// What every land in `queue` is told now: its first ticket that it holds the turn, once, when it
/// takes it; every other who holds the turn and how many are ahead of it.
fn tell(queue: &mut [Entry], now_unix_ms: u64) -> Vec<Notice> {
    let Some((head, waiters)) = queue.split_first_mut() else {
        return Vec::new();
    };
    let mut notices = Vec::with_capacity(waiters.len() + 1);
    if !head.held {
        head.held = true;
        head.held_unix_ms = Some(now_unix_ms);
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

/// Atomic snapshot of the land-turn actor's state, read without fencing (`land-turns`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LandTurnsSnapshot {
    pub queues: Vec<LandTurnQueue>,
    pub waiting_installs: Vec<WaitingInstall>,
    pub parked_asks: Vec<ParkedLandTurn>,
}

/// How an install's wait ended inside the actor.
enum WaitSignal {
    Fenced(Vec<LandTurnQueue>),
    Withdrawn { by_pid: Option<u32> },
}

/// How an install's wait resolved for the control handler.
pub(crate) enum WaitResolution {
    Fenced(Vec<LandTurnQueue>, Fence),
    Withdrawn { by_pid: Option<u32> },
}

enum Message {
    Ask {
        /// The connection that asks, whose departure names it ([`Message::Left`]).
        connection: u64,
        ask: TurnAsk,
        pid: u32,
        answers: mpsc::UnboundedSender<TurnAnswer>,
        /// Told the ticket's number once the actor has given it: at once, or once the ask, parked
        /// behind an install, is numbered.
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
        reply: oneshot::Sender<LandTurnsSnapshot>,
    },
    /// An install's read with the fence: the queues, and whether it fenced them, in one step.
    Fence {
        abandoned_since_unix_ms: u64,
        reply: oneshot::Sender<(Vec<LandTurnQueue>, bool)>,
    },
    /// An install's wait: the queues it waits behind and whether it is fenced at once, and
    /// `fenced` told once it is fenced or withdrawn.
    Wait {
        install: u64,
        pid: Option<u32>,
        abandoned_since_unix_ms: u64,
        reply: oneshot::Sender<(Vec<LandTurnQueue>, bool)>,
        fenced: oneshot::Sender<WaitSignal>,
    },
    /// Withdraw every install currently waiting for the queues to empty (never one already fenced).
    WithdrawWaiting {
        by_pid: Option<u32>,
        reply: oneshot::Sender<Vec<WithdrawnInstall>>,
    },
    /// A fence's connection closed, or a waiting install's: a fenced read's fence is lifted, and an
    /// install that waited, `install`, leaves its wait or lifts the fence it was given. `lifted`
    /// is told once that is done.
    Unfence {
        install: Option<u64>,
        lifted: Option<oneshot::Sender<()>>,
    },
}

/// The land turns' shell: one task owns [`Turns`], fed through this handle.
#[derive(Clone, Debug)]
pub(crate) struct LandTurns {
    messages: mpsc::UnboundedSender<Message>,
    /// Names each land-turn connection; the actor alone gives ticket numbers.
    connections: Arc<AtomicU64>,
    /// How long a `"class":"bounded"` ticket may stay held once taken.
    bounded_deadline: Duration,
}

impl LandTurns {
    #[cfg(test)]
    pub(crate) fn start() -> Self {
        Self::start_with_bounded_deadline(cowshed_core::land_turn::BOUNDED_TURN_DEADLINE)
    }

    pub(crate) fn start_with_bounded_deadline(bounded_deadline: Duration) -> Self {
        let (messages, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run(receiver));
        Self {
            messages,
            connections: Arc::new(AtomicU64::new(0)),
            bounded_deadline,
        }
    }

    /// Every queue with a ticket or a recent abandonment, plus waiting installs and parked asks,
    /// read without fencing; `None` once the task has stopped.
    pub(crate) async fn snapshot(&self, abandoned_since_unix_ms: u64) -> Option<LandTurnsSnapshot> {
        let (reply, answer) = oneshot::channel();
        self.messages
            .send(Message::Queues {
                abandoned_since_unix_ms,
                reply,
            })
            .ok()?;
        answer.await.ok()
    }

    /// Withdraw every install currently waiting for the turns (never one already fenced); `None`
    /// once the task has stopped.
    pub(crate) async fn withdraw_waiting(
        &self,
        by_pid: Option<u32>,
    ) -> Option<Vec<WithdrawnInstall>> {
        let (reply, answer) = oneshot::channel();
        self.messages
            .send(Message::WithdrawWaiting { by_pid, reply })
            .ok()?;
        answer.await.ok()
    }

    /// An install's read of the turns with the fence (05_gateway.md "The read fences the turns"):
    /// every queue, as [`Self::snapshot`] lists them, and, when no queue had a ticket, the [`Fence`]
    /// it set on every queue, held until it is lifted or dropped. `None` once the task has stopped.
    pub(crate) async fn fence(
        &self,
        abandoned_since_unix_ms: u64,
    ) -> Option<(Vec<LandTurnQueue>, Option<Fence>)> {
        let (reply, answer) = oneshot::channel();
        self.messages
            .send(Message::Fence {
                abandoned_since_unix_ms,
                reply,
            })
            .ok()?;
        let (queues, fenced) = answer.await.ok()?;
        Some((
            queues,
            fenced.then(|| Fence {
                turns: Some(self.clone()),
                install: None,
            }),
        ))
    }

    /// An install's wait for the turns (05_gateway.md "The read fences the turns"): every queue as
    /// it begins, whether it is fenced at once, and the [`Waiting`] that becomes its fence once
    /// every ticket queued before it has ended; every ask meanwhile is parked. `None` once the
    /// task has stopped.
    pub(crate) async fn wait(
        &self,
        abandoned_since_unix_ms: u64,
        pid: Option<u32>,
    ) -> Option<(Vec<LandTurnQueue>, bool, Waiting)> {
        let install = self.connections.fetch_add(1, Ordering::Relaxed);
        let (reply, answer) = oneshot::channel();
        let (fenced, fence) = oneshot::channel();
        self.messages
            .send(Message::Wait {
                install,
                pid,
                abandoned_since_unix_ms,
                reply,
                fenced,
            })
            .ok()?;
        // Made before the answer is awaited, so a wait whose service ends meanwhile still leaves.
        let waiting = Waiting {
            turns: Some(self.clone()),
            install,
            fence,
        };
        let (queues, now) = answer.await.ok()?;
        Some((queues, now, waiting))
    }

    fn send(&self, message: Message) {
        // A send fails only once the task has stopped, with the gateway: nothing is held then.
        let _ = self.messages.send(message);
    }
}

/// The fence an install set on every queue, held for as long as its connection is served: dropped
/// on any end of that service, a drain's included, it lifts the fence.
pub(crate) struct Fence {
    /// The turns it fences, until it is lifted.
    turns: Option<LandTurns>,
    /// The install whose wait was given it; `None` for a fenced read's.
    install: Option<u64>,
}

impl Fence {
    /// Lift the fence, returning once the actor has: an ask it reads after this is not fenced.
    pub(crate) async fn lift(mut self) {
        let Some(turns) = self.turns.take() else {
            return;
        };
        let (lifted, done) = oneshot::channel();
        turns.send(Message::Unfence {
            install: self.install,
            lifted: Some(lifted),
        });
        // The actor stops only with the gateway, and no fence outlives it.
        let _ = done.await;
    }
}

impl Drop for Fence {
    fn drop(&mut self) {
        if let Some(turns) = self.turns.take() {
            turns.send(Message::Unfence {
                install: self.install,
                lifted: None,
            });
        }
    }
}

/// An install waiting for the turns, for as long as its connection is served: dropped before it
/// becomes its fence, on any end of that service, a drain's included, it leaves the wait, or lifts
/// the fence it was given and never took.
pub(crate) struct Waiting {
    /// The turns it waits for, until it becomes its fence.
    turns: Option<LandTurns>,
    install: u64,
    fence: oneshot::Receiver<WaitSignal>,
}

impl Waiting {
    /// Resolves once the install is fenced or withdrawn. `None` once the task has stopped.
    pub(crate) async fn wait(mut self) -> Option<WaitResolution> {
        match (&mut self.fence).await.ok()? {
            WaitSignal::Fenced(queues) => Some(WaitResolution::Fenced(
                queues,
                Fence {
                    turns: self.turns.take(),
                    install: Some(self.install),
                },
            )),
            WaitSignal::Withdrawn { by_pid } => {
                // Already removed from `Turns::waiting` by `withdraw_waiting`; disarm Drop.
                self.turns = None;
                Some(WaitResolution::Withdrawn { by_pid })
            }
        }
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        if let Some(turns) = self.turns.take() {
            turns.send(Message::Unfence {
                install: Some(self.install),
                lifted: None,
            });
        }
    }
}
async fn run(mut receiver: mpsc::UnboundedReceiver<Message>) {
    let mut turns = Turns::default();
    let mut answers: HashMap<u64, mpsc::UnboundedSender<TurnAnswer>> = HashMap::new();
    // Each asking connection's ticket number, which its departure names.
    let mut numbers: HashMap<u64, u64> = HashMap::new();
    // Each parked asking connection's answers, and where its number is told once it is given.
    let mut parked: HashMap<u64, (mpsc::UnboundedSender<TurnAnswer>, oneshot::Sender<u64>)> =
        HashMap::new();
    // Each waiting install's listing bound, and where it is told once fenced or withdrawn.
    let mut installs: HashMap<u64, (u64, oneshot::Sender<WaitSignal>)> = HashMap::new();
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
                let ahead_of_install = !turns.waiting.is_empty();
                let inside = turns.inside(pid);
                let bounded = ask.is_bounded();
                match turns.ask(connection, ask, pid, now_unix_ms()) {
                    Ok((given, settled)) => {
                        let why = if !ahead_of_install {
                            if bounded { ", class bounded" } else { "" }
                        } else if inside {
                            ", inside a turn it holds: numbered ahead of the install that \
                             waits for that turn"
                        } else {
                            ", class bounded behind a non-bounded ticket: numbered while an \
                             install waits"
                        };
                        cowshed_os::eprintln!(
                            "cowshed: land turn {repo}: ticket {given} asked by {asked}{why}",
                        );
                        // A connection that has gone hears nothing; its departure is on its way.
                        let _ = number.send(given);
                        numbers.insert(connection, given);
                        answers.insert(given, sender);
                        settled
                    }
                    Err(Parked) => {
                        cowshed_os::eprintln!(
                            "cowshed: land turn {repo}: the ask by {asked} is parked: an install \
                             waits for the turns or fences them, and it is numbered once none does"
                        );
                        let _ = sender.send(TurnAnswer::Fenced);
                        parked.insert(connection, (sender, number));
                        Settled::default()
                    }
                }
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
            } => {
                if let Some(number) = numbers.remove(&connection) {
                    turns.left(number, outcome, now_unix_ms())
                } else {
                    if parked.remove(&connection).is_some() {
                        turns.unpark(connection);
                    }
                    Settled::default()
                }
            }
            Message::Queues {
                abandoned_since_unix_ms,
                reply,
            } => {
                let _ = reply.send(LandTurnsSnapshot {
                    queues: turns.queues(abandoned_since_unix_ms),
                    waiting_installs: turns.waiting_installs(),
                    parked_asks: turns.parked_asks(),
                });
                Settled::default()
            }
            Message::Fence {
                abandoned_since_unix_ms,
                reply,
            } => {
                let queues = turns.queues(abandoned_since_unix_ms);
                let fenced = turns.fence();
                if fenced {
                    cowshed_os::eprintln!(
                        "cowshed: land turns fenced: an install read no holder, and every ask \
                         is parked until its read's connection closes"
                    );
                }
                let _ = reply.send((queues, fenced));
                Settled::default()
            }
            Message::Wait {
                install,
                pid,
                abandoned_since_unix_ms,
                reply,
                fenced,
            } => {
                let queues = turns.queues(abandoned_since_unix_ms);
                let now = turns.wait_fence(install, pid, now_unix_ms());
                if now {
                    cowshed_os::eprintln!(
                        "cowshed: land turns fenced: an install waited for no ticket, and every \
                         ask is parked until its connection closes"
                    );
                    let _ = fenced.send(WaitSignal::Fenced(turns.queues(abandoned_since_unix_ms)));
                } else {
                    cowshed_os::eprintln!(
                        "cowshed: land turns: an install waits for the {} ticket(s) queued now; \
                         every ask from outside their turns is parked until it is fenced and \
                         lifts its fence",
                        queues
                            .iter()
                            .map(|queue| queue.tickets.len())
                            .sum::<usize>()
                    );
                    installs.insert(install, (abandoned_since_unix_ms, fenced));
                }
                let _ = reply.send((queues, now));
                Settled::default()
            }
            Message::WithdrawWaiting { by_pid, reply } => {
                let (withdrawn, settled) = turns.withdraw_waiting(now_unix_ms());
                let mut report = Vec::with_capacity(withdrawn.len());
                for (install, entry) in withdrawn {
                    if let Some((_, signal)) = installs.remove(&install) {
                        let _ = signal.send(WaitSignal::Withdrawn { by_pid });
                    }
                    report.push(entry);
                }
                if !report.is_empty() {
                    cowshed_os::eprintln!(
                        "cowshed: land turns: withdrew {} waiting install(s){}",
                        report.len(),
                        by_pid.map_or_else(String::new, |pid| format!(" (by pid {pid})"))
                    );
                }
                let _ = reply.send(report);
                settled
            }
            Message::Unfence { install, lifted } => {
                let settled = match install {
                    Some(install) => {
                        installs.remove(&install);
                        cowshed_os::eprintln!(
                            "cowshed: land turns: an install that waited has left; its wait or \
                             its fence is gone"
                        );
                        turns.wait_left(install, now_unix_ms())
                    }
                    None => {
                        cowshed_os::eprintln!("cowshed: land turns: an install's fence is lifted");
                        turns.unfence(now_unix_ms())
                    }
                };
                if let Some(lifted) = lifted {
                    let _ = lifted.send(());
                }
                settled
            }
        };
        for install in &settled.fenced {
            if let Some((abandoned_since_unix_ms, fenced)) = installs.remove(install) {
                cowshed_os::eprintln!(
                    "cowshed: land turns fenced: the last ticket queued before a waiting install \
                     has ended, and every ask is parked until its connection closes"
                );
                // An install that has gone hears nothing; its departure lifts the fence.
                let _ = fenced.send(WaitSignal::Fenced(turns.queues(abandoned_since_unix_ms)));
            }
        }
        for &(connection, number) in &settled.numbered {
            if let Some((sender, told)) = parked.remove(&connection) {
                cowshed_os::eprintln!(
                    "cowshed: land turn: a parked ask is numbered ticket {number}"
                );
                // A connection that has gone hears nothing; its departure is on its way.
                let _ = told.send(number);
                numbers.insert(connection, number);
                answers.insert(number, sender);
            }
        }
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
            Some(Ended::Finished {
                ticket,
                outcome,
                bounded,
                held_ms,
            }) => {
                answers.remove(&ticket.number);
                let held = held_ms
                    .map_or_else(String::new, |ms| format!(" after holding the turn {ms} ms"));
                let class = if bounded { ", class bounded" } else { "" };
                cowshed_os::eprintln!(
                    "cowshed: land turn ticket {}: {} (pid {}) finished: {outcome}{held}{class}",
                    ticket.number,
                    ticket.workspace,
                    ticket.pid,
                );
            }
            Some(Ended::Abandoned {
                repo,
                abandonment,
                bounded,
                held_ms,
            }) => {
                answers.remove(&abandonment.ticket.number);
                let held = held_ms
                    .map_or_else(String::new, |ms| format!(" after holding the turn {ms} ms"));
                let class = if bounded { ", class bounded" } else { "" };
                cowshed_os::eprintln!(
                    "cowshed: land turn {repo}: ticket {} of {} (pid {}) abandoned: {}{held}{class}",
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
    let is_bounded = ask.is_bounded();
    let bounded_deadline = turns.bounded_deadline;
    let connection = turns.connections.fetch_add(1, Ordering::Relaxed);
    let (sender, mut answers) = mpsc::unbounded_channel();
    let (numbered, mut number) = oneshot::channel();
    turns.send(Message::Ask {
        connection,
        ask,
        pid: asker.pid().unsigned_abs(),
        answers: sender,
        number: numbered,
    });
    // Placed before its number is awaited, so a hold that ends meanwhile still leaves its queue,
    // or its place among the parked asks.
    let mut place = Place {
        turns: turns.clone(),
        connection,
        outcome: None,
    };
    let asker_gone = async {
        if let Err(error) = exit.observed().await {
            cowshed_os::eprintln!(
                "cowshed: land turn connection {connection}: its asker's exit watch failed \
                 ({error}); its connection alone holds its place"
            );
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(asker_gone);
    let mut steps = BufReader::new(reader).lines();
    // An ask parked behind an install is told `fenced` and waits on this connection for its
    // number, given once no install waits and no fence holds (05_gateway.md "The read fences the
    // turns"). A parked asker sends nothing: its end of file, a broken connection, any step or
    // its exit ends its ask. An ask not parked is numbered at once, and what its asker sent or did
    // meanwhile is read once it is: an end already sent finishes its ticket, never abandons it.
    let mut parked = false;
    let number = loop {
        tokio::select! {
            biased;
            given = &mut number => match given {
                Ok(number) => break number,
                Err(_) => return,
            },
            answer = answers.recv() => {
                let Some(answer) = answer else { return };
                parked |= matches!(answer, TurnAnswer::Fenced);
                if write(&mut writer, &answer).await.is_err() {
                    return;
                }
            }
            _ = steps.next_line(), if parked => return,
            () = &mut asker_gone, if parked => return,
        }
    };
    let mut held = false;
    let mut deadline: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
    loop {
        // Biased, in this order: an asker that sends its end and exits leaves both ready, and its
        // end, already in the socket's buffer, is read before its exit, so the turn records how
        // the command ended rather than an abandonment. An answer that cannot be written means
        // the asker is gone, not that its steps are: what it sent before it went is read below.
        tokio::select! {
            biased;
            answer = answers.recv() => {
                let Some(answer) = answer else { return };
                if matches!(answer, TurnAnswer::Held { .. }) {
                    held = true;
                    if is_bounded && deadline.is_none() {
                        deadline = Some(Box::pin(tokio::time::sleep(bounded_deadline)));
                    }
                }
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
            () = async {
                if let Some(sleep) = deadline.as_mut() {
                    sleep.await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                let bound_ms = u64::try_from(bounded_deadline.as_millis()).unwrap_or(u64::MAX);
                place.outcome = Some(format!(
                    "expired: exceeded bounded-turn deadline ({})",
                    format_bound(bound_ms)
                ));
                let _ = write(&mut writer, &TurnAnswer::Expired { bound_ms }).await;
                let _ = writer.shutdown().await;
                return;
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
mod laws;

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(repo: &str, workspace: &str) -> TurnAsk {
        TurnAsk {
            repo: LandTurnName::new(repo).unwrap(),
            workspace: workspace.to_owned(),
            command: format!("land {workspace}"),
            class: None,
        }
    }

    /// An ask no fence and no waiting install meets, its connection keyed by its pid: its number
    /// and what it settled.
    fn asked(turns: &mut Turns, ask: TurnAsk, pid: u32, asked_unix_ms: u64) -> (u64, Settled) {
        turns
            .ask(u64::from(pid), ask, pid, asked_unix_ms)
            .expect("no install fences the queues or waits for them")
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
        let queues = turns.snapshot(0).await.expect("the land turns run").queues;
        assert!(
            queues.is_empty(),
            "the ticket finished, neither held nor abandoned: {queues:?}"
        );
    }

    #[test]
    fn the_first_ask_holds_the_turn_and_each_later_one_is_told_who_holds_it() {
        let mut turns = Turns::default();
        assert_eq!(
            told(&asked(&mut turns, ask("repo", "a"), 10, 0).1),
            [(1, "held".to_owned())]
        );
        assert_eq!(
            told(&asked(&mut turns, ask("repo", "b"), 20, 1).1),
            [(2, "queued 1 1".to_owned())]
        );
        assert_eq!(
            told(&asked(&mut turns, ask("repo", "c"), 30, 2).1),
            [(2, "queued 1 1".to_owned()), (3, "queued 1 2".to_owned())]
        );
        // Another repository's queue is its own.
        assert_eq!(
            told(&asked(&mut turns, ask("other", "d"), 40, 3).1),
            [(4, "held".to_owned())]
        );
    }

    #[test]
    fn numbers_are_given_in_the_order_asks_arrive_and_each_waits_behind_every_earlier_one() {
        let mut turns = Turns::default();
        let numbers: Vec<u64> = [("repo", "a"), ("other", "b"), ("repo", "c"), ("repo", "d")]
            .into_iter()
            .map(|(repo, workspace)| asked(&mut turns, ask(repo, workspace), 10, 0).0)
            .collect();
        assert_eq!(numbers, [1, 2, 3, 4]);
        assert_eq!(holders(&turns, "repo"), [1, 3, 4]);
        assert_eq!(holders(&turns, "other"), [2]);
    }

    #[test]
    fn a_bound_command_holds_the_turn_past_its_asker_and_its_exit_passes_it_on() {
        let mut turns = Turns::default();
        let (first, _) = asked(&mut turns, ask("repo", "a"), 10, 0);
        let (second, _) = asked(&mut turns, ask("repo", "b"), 20, 1);
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
        let (first, _) = asked(&mut turns, ask("repo", "a"), 10, 0);
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
                    fenced: false,
                },
                outcome: "exited 0".to_owned(),
                bounded: false,
                held_ms: Some(5),
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
        let (first, _) = asked(&mut turns, ask("repo", "a"), 10, 0);
        let (second, _) = asked(&mut turns, ask("repo", "b"), 20, 1);
        let (third, _) = asked(&mut turns, ask("repo", "c"), 30, 2);
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
        let (first, _) = asked(&mut turns, ask("repo", "a"), 10, 0);
        let (second, _) = asked(&mut turns, ask("repo", "b"), 20, 1);
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
            let (number, _) = asked(&mut turns, ask("repo", "a"), 10, at);
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

    //#region smoo/cowshed!n/read-fences-turns
    /// An install's read fences only when no queue has a ticket; an ask under the fence is parked,
    /// never numbered, and changes no queue; once the last fence is lifted the parked ask is
    /// numbered first, and a read that finds a holder sets no fence (05_gateway.md "The read
    /// fences the turns").
    #[test]
    fn a_read_fences_only_free_queues_and_a_fenced_ask_is_parked() {
        let mut turns = Turns::default();
        assert!(turns.fence(), "no ticket: the read fences");
        assert_eq!(turns.ask(7, ask("repo", "a"), 10, 0), Err(Parked));
        assert!(turns.queues(0).is_empty(), "a parked ask is never queued");
        assert!(
            turns.fence(),
            "a parked ask is no ticket: a second read fences too"
        );
        assert_eq!(turns.unfence(1), Settled::default(), "a fence still holds");
        let lifted = turns.unfence(2);
        assert_eq!(
            lifted.numbered,
            [(7, 1)],
            "the parked ask is numbered first"
        );
        assert_eq!(told(&lifted), [(1, "held".to_owned())]);
        assert!(!turns.fence(), "a holder: the read sets no fence");
        assert_eq!(asked(&mut turns, ask("other", "b"), 20, 2).0, 2);
    }

    /// The install whose wait the actor keys.
    const INSTALL: u64 = 900;

    /// An install that finds tickets queued waits for them in order (05_gateway.md "The read
    /// fences the turns"): lands keep asking while it waits, and each is parked, never numbered,
    /// so the install is fenced in the very step the last ticket queued before it ends, however
    /// many asks arrive meanwhile. Once its fence lifts, the parked asks are numbered in the order
    /// they arrived and enter their queues.
    #[test]
    fn a_waiting_install_is_fenced_after_the_tickets_before_it_and_parked_asks_follow_it() {
        let mut turns = Turns::default();
        let (first, _) = asked(&mut turns, ask("repo", "a"), 10, 0);
        let (second, _) = asked(&mut turns, ask("other", "b"), 20, 1);
        assert!(
            !turns.wait_fence(INSTALL, None, 1),
            "tickets are queued: the install waits"
        );
        for (key, workspace) in [(3, "c"), (4, "d"), (5, "e")] {
            assert_eq!(
                turns.ask(key, ask("repo", workspace), 30, 2),
                Err(Parked),
                "an ask while an install waits is parked"
            );
        }
        assert_eq!(
            holders(&turns, "repo"),
            [first],
            "no parked ask is numbered"
        );
        let ended = turns.left(first, Some("exited 0".to_owned()), 3);
        assert!(
            ended.fenced.is_empty(),
            "a ticket queued before the install is left"
        );
        let ended = turns.left(second, Some("exited 0".to_owned()), 4);
        assert_eq!(
            ended.fenced,
            [INSTALL],
            "the step that ends the last ticket fences the install"
        );
        assert_eq!(turns.fences, 1);
        assert!(
            turns.queues(u64::MAX).is_empty(),
            "the parked asks wait behind the install's fence"
        );
        let lifted = turns.unfence(5);
        assert_eq!(
            lifted.numbered,
            [(3, 3), (4, 4), (5, 5)],
            "the parked asks are numbered in arrival order"
        );
        assert_eq!(holders(&turns, "repo"), [3, 4, 5]);
        assert_eq!(told(&lifted).first(), Some(&(3, "held".to_owned())));
        assert_eq!(told(&lifted).last(), Some(&(5, "queued 3 2".to_owned())));
    }

    /// A waiting install that leaves before it is fenced admits the asks parked behind it, and a
    /// parked asker that left is never numbered; on free queues a waiting install is fenced at
    /// once, and its fence holds the asks after it back until it lifts.
    #[test]
    fn a_waiting_install_that_leaves_admits_the_asks_parked_behind_it() {
        let mut turns = Turns::default();
        let (first, _) = asked(&mut turns, ask("repo", "a"), 10, 0);
        assert!(!turns.wait_fence(INSTALL, None, 0));
        assert_eq!(turns.ask(2, ask("repo", "b"), 20, 1), Err(Parked));
        assert_eq!(turns.ask(3, ask("repo", "c"), 30, 2), Err(Parked));
        turns.unpark(2);
        let left = turns.wait_left(INSTALL, 2);
        assert_eq!(
            left.numbered,
            [(3, 2)],
            "the parked asker that left is never numbered"
        );
        assert_eq!(holders(&turns, "repo"), [first, 2]);
        assert_eq!(
            turns.wait_left(INSTALL, 2),
            Settled::default(),
            "an install that has left leaves again as a no-op"
        );
        assert_eq!(
            (holders(&turns, "repo"), turns.fences, turns.numbered),
            (vec![first, 2], 0, 2),
            "and changes nothing"
        );
        turns.left(first, Some("exited 0".to_owned()), 3);
        turns.left(2, Some("exited 0".to_owned()), 4);
        assert!(
            turns.wait_fence(INSTALL + 1, None, 4),
            "no ticket: the install is fenced at once"
        );
        assert_eq!(turns.ask(4, ask("repo", "d"), 40, 5), Err(Parked));
        assert_eq!(turns.wait_left(INSTALL + 1, 6).numbered, [(4, 3)]);
        assert_eq!(
            turns.wait_left(INSTALL + 1, 6),
            Settled::default(),
            "a fence lifted once is lifted"
        );
    }
    //#endregion smoo/cowshed!n/read-fences-turns

    /// A ticket whose asker has already ended or exited (held only by a bound command) has no live
    /// asker to acknowledge a fence and is already fenced (`ticket_fenced` in `land_turn.bend`),
    /// so a waiting install or fenced read fences at once without waiting for that bound command
    /// to exit (05_gateway.md "The read fences the turns").
    #[test]
    fn a_ticket_held_only_by_a_bound_command_is_already_fenced_for_a_waiting_install() {
        let mut turns = Turns::default();
        let (first, _) = asked(&mut turns, ask("repo", "a"), 10, 0);
        turns.bind(first, 11).unwrap();
        assert_eq!(
            turns
                .left(first, Some("command runs on".to_owned()), 2)
                .ended,
            None
        );
        assert!(
            turns.wait_fence(INSTALL, None, 3),
            "a ticket with no live asker is already fenced: the waiting install must fence at once"
        );
        assert!(
            turns.fence(),
            "a fenced read must also fence when every live ticket has no live asker"
        );
        assert_eq!(
            holders(&turns, "repo"),
            [first],
            "the bound command still holds its ticket in the queue"
        );
    }

    async fn read_json_line(
        lines: &mut tokio::io::Lines<BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
        what: &str,
    ) -> serde_json::Value {
        let line = tokio::time::timeout(cowshed_os::test_backstop::NO_PROGRESS, lines.next_line())
            .await
            .unwrap_or_else(|_| panic!("{what} timed out at the no-progress backstop"))
            .unwrap_or_else(|error| panic!("reading {what}: {error}"))
            .unwrap_or_else(|| panic!("{what}: connection closed before answering"));
        serde_json::from_str(&line)
            .unwrap_or_else(|error| panic!("{what}: bad JSON {line:?}: {error}"))
    }

    /// Waiting install fences once every held and queued resumable turn acknowledges
    /// `{"step":"fenced"}`, without emptying the queue (`install_fenced_when_all_turns_fence`,
    /// 05_gateway.md "The read fences the turns").
    #[tokio::test]
    async fn a_waiting_install_fences_once_every_held_and_queued_turn_acknowledges_without_emptying_the_queue()
     {
        let turns = LandTurns::start();
        let (_peer_a, sock_a) = std::os::unix::net::UnixStream::pair().expect("socket pair a");
        let asker_a = Process::of_socket_peer(&sock_a).expect("peer a");
        let (client_a, server_a) = tokio::io::duplex(4096);
        let (srv_a_r, srv_a_w) = tokio::io::split(server_a);
        let (cli_a_r, mut cli_a_w) = tokio::io::split(client_a);
        let mut lines_a = BufReader::new(cli_a_r).lines();
        let task_a = tokio::spawn(hold(
            srv_a_r,
            srv_a_w,
            Some(asker_a),
            ask("repo", "a"),
            turns.clone(),
        ));
        let held_a = read_json_line(&mut lines_a, "ticket a held").await;
        assert_eq!(held_a["turn"], serde_json::json!("held"), "{held_a}");

        let (_peer_b, sock_b) = std::os::unix::net::UnixStream::pair().expect("socket pair b");
        let asker_b = Process::of_socket_peer(&sock_b).expect("peer b");
        let (client_b, server_b) = tokio::io::duplex(4096);
        let (srv_b_r, srv_b_w) = tokio::io::split(server_b);
        let (cli_b_r, mut cli_b_w) = tokio::io::split(client_b);
        let mut lines_b = BufReader::new(cli_b_r).lines();
        let task_b = tokio::spawn(hold(
            srv_b_r,
            srv_b_w,
            Some(asker_b),
            ask("repo", "b"),
            turns.clone(),
        ));
        let queued_b = read_json_line(&mut lines_b, "ticket b queued").await;
        assert_eq!(queued_b["turn"], serde_json::json!("queued"), "{queued_b}");

        let (_initial, fenced_now, waiting) = turns
            .wait(u64::MAX, Some(std::process::id()))
            .await
            .expect("wait_fence");
        assert!(!fenced_now, "two unfenced tickets are live");

        // Both held and queued askers acknowledge `{"step":"fenced"}`.
        cli_a_w
            .write_all(b"{\"step\":\"fenced\"}\n")
            .await
            .expect("send fenced ack for a");
        cli_b_w
            .write_all(b"{\"step\":\"fenced\"}\n")
            .await
            .expect("send fenced ack for b");

        let resolved = tokio::time::timeout(cowshed_os::test_backstop::NO_PROGRESS, waiting.wait())
            .await
            .expect("waiting install must be fenced once both tickets acknowledge")
            .expect("wait resolution");
        match resolved {
            WaitResolution::Fenced(queues, _fence) => {
                let tickets = &queues
                    .iter()
                    .find(|q| q.repo.as_str() == "repo")
                    .expect("repo queue must stay populated")
                    .tickets;
                assert_eq!(
                    tickets
                        .iter()
                        .map(|t| (t.workspace.as_str(), t.fenced))
                        .collect::<Vec<_>>(),
                    [("a", true), ("b", true)],
                    "both tickets must remain in the queue marked fenced"
                );
            }
            WaitResolution::Withdrawn { .. } => panic!("install was withdrawn instead of fenced"),
        }

        let _ = cli_a_w.shutdown().await;
        let _ = cli_b_w.shutdown().await;
        drop(lines_a);
        drop(lines_b);
        drop(cli_a_w);
        drop(cli_b_w);
        let _ = tokio::time::timeout(cowshed_os::test_backstop::NO_PROGRESS, task_a)
            .await
            .expect("task_a exits on client close");
        let _ = tokio::time::timeout(cowshed_os::test_backstop::NO_PROGRESS, task_b)
            .await
            .expect("task_b exits on client close");
    }

    /// A fenced turn refuses new binds (`fenced_bind_refused`, 05_gateway.md "Land turns") until
    /// it resumes or unfences.
    #[tokio::test]
    async fn a_fenced_turn_refuses_new_binds_until_resumed_or_unfenced() {
        let turns = LandTurns::start();
        let (_peer, sock) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let asker = Process::of_socket_peer(&sock).expect("peer");
        let (client, server) = tokio::io::duplex(4096);
        let (srv_r, srv_w) = tokio::io::split(server);
        let (cli_r, mut cli_w) = tokio::io::split(client);
        let mut lines = BufReader::new(cli_r).lines();
        let task = tokio::spawn(hold(
            srv_r,
            srv_w,
            Some(asker),
            ask("repo", "a"),
            turns.clone(),
        ));

        let held = read_json_line(&mut lines, "held").await;
        assert_eq!(held["turn"], serde_json::json!("held"), "{held}");

        let mut child = tokio::process::Command::new("sh")
            .args(["-c", "read line"])
            .stdin(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn child to bind");
        let child_pid = child.id().expect("child pid");

        cli_w
            .write_all(b"{\"step\":\"fenced\"}\n")
            .await
            .expect("send fenced");
        cli_w
            .write_all(format!("{{\"step\":\"bind\",\"pid\":{child_pid}}}\n").as_bytes())
            .await
            .expect("send bind while fenced");

        let answer = read_json_line(&mut lines, "bind refusal while fenced").await;
        assert_eq!(
            answer["turn"],
            serde_json::json!("refused"),
            "bind on a fenced turn must be refused, not granted: {answer}"
        );
        assert!(
            answer["error"]
                .as_str()
                .is_some_and(|msg| msg.contains("fenced")
                    && !msg.contains("unknown variant")
                    && !msg.contains("a land turn step is a bind or an end")),
            "bind refusal must name fenced state, not reject step fenced as unknown: {answer}"
        );
        let snap = turns.snapshot(u64::MAX).await.expect("snapshot");
        assert!(
            snap.queues[0].tickets[0].fenced,
            "ticket must be marked fenced: {snap:?}"
        );
        assert!(
            snap.queues[0].tickets[0].bound.is_empty(),
            "refused bind must not bind the child pid: {snap:?}"
        );

        let _ = cli_w.shutdown().await;
        drop(lines);
        drop(cli_w);
        let _ = child.kill().await;
        let _ = tokio::time::timeout(cowshed_os::test_backstop::NO_PROGRESS, task)
            .await
            .expect("hold task exits on client close");
    }

    /// When an install lifts its fence without restarting, held fenced turns receive
    /// `{"turn":"unfenced"}` and resume in place (`unfence_clears_fenced`, 05_gateway.md "The read
    /// fences the turns").
    #[tokio::test]
    async fn an_install_that_lifts_its_fence_without_restarting_unfences_held_turns() {
        let turns = LandTurns::start();
        let (_peer, sock) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let asker = Process::of_socket_peer(&sock).expect("peer");
        let (client, server) = tokio::io::duplex(4096);
        let (srv_r, srv_w) = tokio::io::split(server);
        let (cli_r, mut cli_w) = tokio::io::split(client);
        let mut lines = BufReader::new(cli_r).lines();
        let task = tokio::spawn(hold(
            srv_r,
            srv_w,
            Some(asker),
            ask("repo", "a"),
            turns.clone(),
        ));

        let held = read_json_line(&mut lines, "held").await;
        assert_eq!(held["turn"], serde_json::json!("held"), "{held}");

        let (_queues, fenced_now, waiting) = turns
            .wait(u64::MAX, Some(std::process::id()))
            .await
            .expect("wait_fence");
        assert!(!fenced_now);

        let fence_notice = read_json_line(&mut lines, "fence notice").await;
        assert_eq!(
            fence_notice["turn"],
            serde_json::json!("fence"),
            "waiting install must ask live resumable holder to fence: {fence_notice}"
        );
        cli_w
            .write_all(b"{\"step\":\"fenced\"}\n")
            .await
            .expect("ack fence");
        let fence =
            match tokio::time::timeout(cowshed_os::test_backstop::NO_PROGRESS, waiting.wait())
                .await
                .expect("waiting install fences")
                .expect("wait resolution")
            {
                WaitResolution::Fenced(_, fence) => fence,
                WaitResolution::Withdrawn { .. } => panic!("unexpected withdrawal"),
            };

        // Install lifts its fence without restarting.
        fence.lift().await;
        let unfenced = read_json_line(&mut lines, "unfenced notice").await;
        assert_eq!(
            unfenced["turn"],
            serde_json::json!("unfenced"),
            "lifting the fence without restart must unmark held turns in place: {unfenced}"
        );
        let snap = turns.snapshot(u64::MAX).await.expect("snapshot");
        assert!(
            !snap.queues[0].tickets[0].fenced,
            "ticket fenced mark must clear after unfence: {snap:?}"
        );

        let _ = cli_w.shutdown().await;
        drop(lines);
        drop(cli_w);
        let _ = tokio::time::timeout(cowshed_os::test_backstop::NO_PROGRESS, task)
            .await
            .expect("hold task exits on client close");
    }
}
