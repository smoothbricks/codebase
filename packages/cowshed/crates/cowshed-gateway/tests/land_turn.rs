#![cfg(target_os = "macos")]
//! Land turns over a real control socket (05_gateway.md "Land turns"): the gateway keeps every
//! repository's queue, first asked first served, and holds each turn by processes: its asker
//! while it holds its place, and each command bound to it until that command exits. Nothing a
//! bound command leaves running holds the turn.

#[path = "support/fixture_dir.rs"]
mod fixture_dir;

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cowshed_core::api::dto::{LandTurnName, LandTurnQueue};
use cowshed_core::land_turn::{self, Turn, TurnAnswer, TurnAsk, Wait};
use cowshed_core::runtime::ref_turn::{RefTurn, RefWriter};
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, CredentialError,
    CredentialProvider, CredentialQuery, CredentialRecord, Gateway, GatewayConfig,
    GatewayControlClient, MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
};
use cowshed_os::test_backstop::{
    AsyncChildBackstop as _, AsyncUnixStreamBackstop as _, ChildBackstop as _,
};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::net::UnixStream;
use uuid::Uuid;

struct NoCredentials;

#[async_trait]
impl CredentialProvider for NoCredentials {
    async fn lookup(
        &self,
        _query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        Ok(None)
    }
}

struct NoConnector;

#[async_trait]
impl UpstreamConnector for NoConnector {
    async fn health(&self, _target: &cowshed_gateway::CanonicalTarget) -> UpstreamHealth {
        UpstreamHealth::Unknown
    }

    async fn connect(
        &self,
        _target: &AuthorizedTarget,
    ) -> Result<UpstreamConnection, ConnectError> {
        Err(ConnectError::NoAddresses)
    }
}

struct DiscardAudit;

#[async_trait]
impl AuditSink for DiscardAudit {
    async fn record(&self, _event: AuditEvent) -> Result<(), AuditError> {
        Ok(())
    }

    async fn flush(&self) -> Result<(), AuditError> {
        Ok(())
    }
}

/// A gateway on a socket under the scratch parent, and the guard that removes its root.
async fn gateway() -> (Gateway, fixture_dir::FixtureDir, PathBuf) {
    let id = Uuid::new_v4().simple().to_string();
    let root = fixture_dir::secure_fixture_dir(&format!("csturn-{}", &id[..8]));
    let cache = root.path().join("cache");
    std::fs::create_dir(&cache).expect("cache");
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).expect("secure cache");
    let socket = root.path().join("gateway.sock");
    let gateway = Gateway::start(
        GatewayConfig {
            control_socket: Some(socket.clone()),
            listener_handoff: Some(cowshed_gateway::handoff_path(&socket)),
            mirror_cache: MirrorCacheConfig::new(cache),
            ..GatewayConfig::default()
        },
        Arc::new(NoCredentials),
        Arc::new(NoConnector),
        Arc::new(DiscardAudit),
    )
    .await
    .expect("start gateway");
    (gateway, root, socket)
}

const REPO: &str = "acme";

fn ask(workspace: &str) -> TurnAsk {
    TurnAsk {
        repo: LandTurnName::new(REPO).unwrap(),
        workspace: workspace.to_owned(),
        command: format!("land {workspace}"),
    }
}

/// `step` awaited to its end. A step that never ends fails the test at the run's no-progress
/// backstop, the one bound that decides a hang ([`cowshed_os::test_backstop`]), naming `what`.
async fn awaiting<T>(what: &str, step: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(cowshed_os::test_backstop::NO_PROGRESS, step)
        .await
        .unwrap_or_else(|_| panic!("{what} did not happen before the no-progress backstop"))
}

/// Take `workspace`'s turn, failing if it has to wait.
async fn take_free(socket: &Path, workspace: &str) -> Turn {
    take_asked(socket, &ask(workspace)).await
}

/// Take the turn `ask` asks for, failing if it has to wait.
async fn take_asked(socket: &Path, ask: &TurnAsk) -> Turn {
    let workspace = ask.workspace.clone();
    awaiting(
        "a free turn",
        land_turn::take(socket, ask, None, Duration::from_secs(90), |wait| {
            panic!("{workspace} waited for a free turn: {wait:?}")
        }),
    )
    .await
    .expect("take a free turn")
}

/// The repository's queue as the gateway lists it, with every abandonment it kept.
async fn queue(socket: &Path) -> Option<LandTurnQueue> {
    awaiting(
        "the land turns listing",
        GatewayControlClient::new(socket.to_path_buf())
            .expect("client")
            .land_turns(0),
    )
    .await
    .expect("land turns")
    .into_iter()
    .find(|queue| queue.repo.as_str() == REPO)
}

/// The workspaces of the repository's tickets, in turn order.
async fn tickets(socket: &Path) -> Vec<String> {
    queue(socket).await.map_or_else(Vec::new, |queue| {
        queue
            .tickets
            .into_iter()
            .map(|ticket| ticket.workspace)
            .collect()
    })
}

/// Take `workspace`'s turn once every ticket asked before it has gone: the gateway tells it `held`
/// only after it has processed the last of their departures, so a listing read while it holds the
/// turn shows every one of them.
async fn take_after(socket: &Path, workspace: &str) -> Turn {
    awaiting(
        &format!("{workspace}'s turn"),
        land_turn::take(
            socket,
            &ask(workspace),
            None,
            Duration::from_secs(90),
            |_| {},
        ),
    )
    .await
    .expect("take the turn")
}

/// The next answer the gateway writes on a land-turn connection read raw.
async fn next_answer(answers: &mut Lines<BufReader<UnixStream>>) -> TurnAnswer {
    let line = awaiting("the gateway's next answer", answers.next_line())
        .await
        .expect("read the gateway's answer")
        .expect("the gateway answers");
    serde_json::from_str(&line).expect("a land-turn answer")
}

/// A turn waiting in the background, with every wait it is told, in order.
fn waiting(
    socket: &Path,
    workspace: &str,
) -> (
    tokio::task::JoinHandle<Turn>,
    tokio::sync::mpsc::UnboundedReceiver<Wait>,
) {
    let (told, waits) = tokio::sync::mpsc::unbounded_channel();
    let socket = socket.to_path_buf();
    let ask = ask(workspace);
    let taking = tokio::spawn(async move {
        land_turn::take(&socket, &ask, None, Duration::from_secs(90), move |wait| {
            // A test that has stopped reading the waits has what it needed of them.
            let _ = told.send(wait.clone());
        })
        .await
        .expect("take a turn")
    });
    (taking, waits)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turns_pass_in_the_order_asked_and_a_waiter_is_told_who_holds_the_turn() {
    let (_gateway, _root, socket) = gateway().await;
    let first = take_free(&socket, "first").await;
    let (second, mut told_second) = waiting(&socket, "second");
    let Wait::Queued { holder, ahead } = awaiting("second's first wait", told_second.recv())
        .await
        .expect("second is told")
    else {
        panic!("second waits for the queue")
    };
    assert_eq!(
        (holder.workspace.as_str(), holder.pid, ahead),
        ("first", std::process::id(), 1)
    );
    let (third, mut told_third) = waiting(&socket, "third");
    let Wait::Queued { ahead, .. } = awaiting("third's first wait", told_third.recv())
        .await
        .expect("third is told")
    else {
        panic!("third waits for the queue")
    };
    assert_eq!(ahead, 2);
    assert_eq!(tickets(&socket).await, ["first", "second", "third"]);
    first.end("exited 0".to_owned());
    let second = awaiting("second's turn", second)
        .await
        .expect("second's turn");
    assert!(!third.is_finished(), "third took the turn ahead of second");
    second.end("exited 0".to_owned());
    awaiting("third's turn", third)
        .await
        .expect("third's turn")
        .end("exited 0".to_owned());
    let probe = take_after(&socket, "probe").await;
    let listed = queue(&socket).await.expect("the probe's queue is listed");
    assert!(
        listed.abandoned.is_empty(),
        "an ended turn is no abandonment: {:?}",
        listed.abandoned
    );
    probe.end("exited 0".to_owned());
}

/// Measured on the installed cowshed 2026-10-26: a second land waited 19 s for a `sleep 20` the
/// first land's command left running, and its waiting line named the first land's exited pid.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_process_the_bound_command_leaves_running_holds_no_turn() {
    let (_gateway, _root, socket) = gateway().await;
    let mut first = take_free(&socket, "first").await;
    let mut command = tokio::process::Command::new("sh");
    command
        .args(["-c", "sleep 600 </dev/null >/dev/null 2>&1 & echo $!"])
        .stdout(std::process::Stdio::piped());
    first.bind_at_exec(command.as_std_mut());
    let command = command.spawn().expect("spawn the command");
    awaiting(
        "the command's bind",
        first.bound(command.id().expect("a running command has a pid")),
    )
    .await
    .expect("bind the command");
    let output = awaiting("the command's end", command.wait_with_output_bounded())
        .await
        .expect("the command ends");
    let descendant: i32 = String::from_utf8(output.stdout)
        .expect("a pid")
        .trim()
        .parse()
        .expect("a pid");
    first.end("exited 0".to_owned());
    // Second holds the turn once first's departure is processed, the descendant still running.
    let second = take_after(&socket, "second").await;
    // SAFETY: kill(2) with signal 0 only checks that the process exists.
    assert_eq!(
        unsafe { libc::kill(descendant, 0) },
        0,
        "the descendant still runs"
    );
    second.end("exited 0".to_owned());
    // SAFETY: kill(2) of the descendant this test started.
    assert_eq!(unsafe { libc::kill(descendant, libc::SIGKILL) }, 0);
}

/// The asker's hold ends without the command's: a turn whose asker ended it stays held until the
/// command bound to it exits, and the next land is told the holder's command pid meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bound_command_holds_the_turn_after_its_asker_ends_its_hold() {
    let (_gateway, _root, socket) = gateway().await;
    let mut first = take_free(&socket, "first").await;
    let mut command = Command::new("sh");
    command
        .args(["-c", "read line"])
        .stdin(std::process::Stdio::piped());
    first.bind_at_exec(&mut command);
    let mut command = command.spawn().expect("spawn the command");
    awaiting("the command's bind", first.bound(command.id()))
        .await
        .expect("bind the command");
    first.end("its command runs on".to_owned());
    let (second, mut told) = waiting(&socket, "second");
    let Wait::Queued { holder, .. } = awaiting("second's first wait", told.recv())
        .await
        .expect("second is told")
    else {
        panic!("second waits for the queue")
    };
    assert_eq!(holder.bound, [command.id()]);
    assert!(
        !second.is_finished(),
        "the turn passed on while its command ran"
    );
    drop(command.stdin.take());
    command.wait_bounded().expect("the command ends");
    awaiting("second's turn", second)
        .await
        .expect("second's turn")
        .end("exited 0".to_owned());
}

/// Measured 2026-10-26 in review: a land killed between spawning its gate and binding it left the
/// gate running outside the turn. A command binds itself as it starts, so even a command that
/// execs at once and its asker's departure right after leave the turn held until it exits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_command_holds_the_turn_from_its_start_whatever_its_asker_does_next() {
    let (_gateway, _root, socket) = gateway().await;
    let first = take_free(&socket, "first").await;
    let mut command = Command::new("sh");
    command
        .args(["-c", "exec cat >/dev/null"])
        .stdin(std::process::Stdio::piped());
    first.bind_at_exec(&mut command);
    let mut command = command.spawn().expect("spawn the command");
    // The asker goes without reading the gateway's answer, as a land killed after its spawn does.
    drop(first);
    let pid = command.id();
    let (second, mut told) = waiting(&socket, "second");
    // Second is told again whenever its queue changes, the command's bind included; had the
    // asker's departure been read before that bind, the turn would pass to second, ending its
    // waits.
    awaiting(
        "second told the command holds the turn, its bind read before the asker's departure",
        async {
            loop {
                match told
                    .recv()
                    .await
                    .expect("second waits while the command runs")
                {
                    Wait::Queued { holder, .. } if holder.bound == [pid] => break,
                    Wait::Queued { .. } => {}
                    Wait::Outside { .. } => panic!("second waits for the queue"),
                }
            }
        },
    )
    .await;
    drop(command.stdin.take());
    command.wait_bounded().expect("the command ends");
    awaiting("second's turn", second)
        .await
        .expect("second's turn")
        .end("exited 0".to_owned());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiter_that_leaves_blocks_nobody_and_is_listed_abandoned() {
    let (_gateway, _root, socket) = gateway().await;
    let first = take_free(&socket, "first").await;
    let (second, mut told_second) = waiting(&socket, "second");
    awaiting("second's first wait", told_second.recv())
        .await
        .expect("second is told");
    let (third, mut told_third) = waiting(&socket, "third");
    awaiting("third's first wait", told_third.recv())
        .await
        .expect("third is told");
    second.abort();
    // Third is told one fewer ahead once the gateway has taken second out of the queue.
    let Wait::Queued { ahead, .. } = awaiting("third told second left", told_third.recv())
        .await
        .expect("third is told")
    else {
        panic!("third waits for the queue")
    };
    assert_eq!(ahead, 1);
    assert_eq!(tickets(&socket).await, ["first", "third"]);
    first.end("exited 0".to_owned());
    awaiting("third's turn", third)
        .await
        .expect("third's turn")
        .end("exited 0".to_owned());
    let probe = take_after(&socket, "probe").await;
    let abandoned = queue(&socket)
        .await
        .expect("the abandonment is listed")
        .abandoned;
    assert_eq!(
        abandoned
            .iter()
            .map(|gone| (gone.ticket.workspace.as_str(), gone.reason.as_str()))
            .collect::<Vec<_>>(),
        [("second", "it left the queue before its turn")]
    );
    probe.end("exited 0".to_owned());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_land_that_asks_for_a_held_ref_turn_holds_it_once_the_writer_ends() {
    let (_gateway, _root, socket) = gateway().await;
    let queue_name = LandTurnName::new("acme.ref").unwrap();
    let writer = awaiting(
        "the writer's ref turn",
        RefTurn::acquire_in(&socket, &queue_name, RefWriter::DocCommit),
    )
    .await
    .expect("the writer takes a free ref turn");
    // A probe asked behind the writer and read raw hears every change of its queue, a ticket
    // asked behind it included (`land_turn::take` tells a wait only when it changes).
    let mut probe = UnixStream::connect_bounded(&socket)
        .await
        .expect("connect the probe");
    let probe_ask = serde_json::json!({
        "op": land_turn::LAND_TURN_OP,
        "repo": queue_name.as_str(),
        "workspace": "probe",
        "command": "probe",
    });
    probe
        .write_all(format!("{probe_ask}\n").as_bytes())
        .await
        .expect("ask the probe's turn");
    let mut answers = BufReader::new(probe).lines();
    let TurnAnswer::Queued { .. } = next_answer(&mut answers).await else {
        panic!("the probe waits behind the writer")
    };
    let land = tokio::spawn({
        let socket = socket.clone();
        let queue_name = queue_name.clone();
        async move {
            RefTurn::acquire_in(&socket, &queue_name, RefWriter::Land { workspace: "topic" }).await
        }
    });
    // The probe's next answer is the land's ask, queued behind it.
    let TurnAnswer::Queued { .. } = next_answer(&mut answers).await else {
        panic!("the probe still waits behind the writer")
    };
    let listed = awaiting(
        "the land turns listing",
        GatewayControlClient::new(socket.clone())
            .expect("client")
            .land_turns(u64::MAX),
    )
    .await
    .expect("land turns")
    .into_iter()
    .find(|queue| queue.repo == queue_name)
    .expect("the ref turn's queue is listed");
    assert_eq!(
        listed
            .tickets
            .iter()
            .map(|ticket| ticket.workspace.as_str())
            .collect::<Vec<_>>(),
        ["doc-commit", "probe", "topic"]
    );
    assert!(!land.is_finished(), "the land took a held ref turn");
    // The probe leaves the queue: the land waits for the writer alone.
    drop(answers);
    writer.end("committed".to_owned());
    awaiting("the land's ref turn", land)
        .await
        .expect("the land task")
        .expect("the land takes the released ref turn")
        .end("delivered".to_owned());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gateway_that_does_not_answer_refuses_the_turn() {
    let (_gateway, root, _socket) = gateway().await;
    let absent = root.path().join("absent.sock");
    let refused = land_turn::take(
        &absent,
        &ask("first"),
        None,
        Duration::from_secs(90),
        |_| {},
    )
    .await
    .expect_err("no gateway answers");
    assert!(
        refused.to_string().contains("cannot reach the gateway"),
        "{refused}"
    );
}
