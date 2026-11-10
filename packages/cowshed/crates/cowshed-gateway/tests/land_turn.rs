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
use cowshed_core::land_turn::{self, Turn, TurnAnswer, TurnAsk, TurnGateway, Wait};
use cowshed_core::runtime::ref_turn::{RefTurn, RefWriter};
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, CredentialError,
    CredentialProvider, CredentialQuery, CredentialRecord, Gateway, GatewayConfig,
    GatewayControlClient, MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
};
use cowshed_os::test_backstop::{
    AsyncChildBackstop as _, AsyncUnixStreamBackstop as _, ChildBackstop as _,
};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader, Lines};
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
    (gateway_at(&socket, &cache).await, root, socket)
}

/// A gateway serving `socket`, its mirror cache in `cache`: a restart starts one where another
/// served.
async fn gateway_at(socket: &Path, cache: &Path) -> Gateway {
    Gateway::start(
        GatewayConfig {
            control_socket: Some(socket.to_path_buf()),
            listener_handoff: Some(cowshed_gateway::handoff_path(socket)),
            mirror_cache: MirrorCacheConfig::new(cache.to_path_buf()),
            ..GatewayConfig::default()
        },
        Arc::new(NoCredentials),
        Arc::new(NoConnector),
        Arc::new(DiscardAudit),
    )
    .await
    .expect("start gateway")
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

/// The gateway at `socket` as a land asks it, its install slot in a `support` directory beside it.
fn turn_gateway(socket: &Path) -> TurnGateway {
    let namespace = cowshed_os::namespace::TestNamespace::of(std::process::id(), 0)
        .expect("this process's namespace");
    TurnGateway {
        socket: socket.to_path_buf(),
        support: socket
            .parent()
            .expect("the socket's directory")
            .join("support"),
        census: cowshed_core::census::CensusScope::new(
            cowshed_os::namespace::HostNamespace::Test(namespace),
            Arc::from(Path::new("/var/empty")),
        ),
    }
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
        land_turn::take(
            &turn_gateway(socket),
            ask,
            None,
            Duration::from_secs(90),
            |wait| panic!("{workspace} waited for a free turn: {wait:?}"),
        ),
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
            &turn_gateway(socket),
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
        land_turn::take(
            &turn_gateway(&socket),
            &ask,
            None,
            Duration::from_secs(90),
            move |wait| {
                // A test that has stopped reading the waits has what it needed of them.
                let _ = told.send(wait.clone());
            },
        )
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
                    other @ (Wait::Outside { .. } | Wait::Install { .. }) => {
                        panic!("second waits for the queue, not {other:?}")
                    }
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
        RefTurn::acquire_in(&turn_gateway(&socket), &queue_name, RefWriter::DocCommit),
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
            RefTurn::acquire_in(
                &turn_gateway(&socket),
                &queue_name,
                RefWriter::Land { workspace: "topic" },
            )
            .await
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
        &turn_gateway(&absent),
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

/// The install's read of the land turns as `start_service` asks it, with the fence, and the
/// connection that holds the fence for as long as it is open (05_gateway.md "The read fences the
/// turns").
async fn fenced_read(socket: &Path) -> (serde_json::Value, UnixStream) {
    let mut install = UnixStream::connect_bounded(socket)
        .await
        .expect("connect the install's read");
    let read = serde_json::json!({
        "op": land_turn::LAND_TURNS_OP,
        "abandonedSinceUnixMs": u64::MAX,
        "fence": true,
    });
    install
        .write_all(format!("{read}\n").as_bytes())
        .await
        .expect("send the install's read");
    let mut answer = String::new();
    awaiting(
        "the install's read",
        BufReader::new(&mut install).read_line(&mut answer),
    )
    .await
    .expect("read the install's answer");
    (
        serde_json::from_str(&answer).expect("a control answer"),
        install,
    )
}

/// A land's ask written raw, and the connection its answers come on.
async fn ask_raw(socket: &Path, workspace: &str) -> Lines<BufReader<UnixStream>> {
    let mut land = UnixStream::connect_bounded(socket)
        .await
        .expect("connect the land");
    let asked = serde_json::json!({
        "op": land_turn::LAND_TURN_OP,
        "repo": REPO,
        "workspace": workspace,
        "command": format!("land {workspace}"),
    });
    land.write_all(format!("{asked}\n").as_bytes())
        .await
        .expect("ask the turn");
    BufReader::new(land).lines()
}

/// The next answer on a raw land-turn connection, as JSON.
async fn raw_answer(answers: &mut Lines<BufReader<UnixStream>>) -> serde_json::Value {
    let line = awaiting("the gateway's next answer", answers.next_line())
        .await
        .expect("read the gateway's answer")
        .expect("the gateway answers");
    serde_json::from_str(&line).expect("a land-turn answer")
}

//#region smoo/cowshed!n/read-fences-turns
/// The install's fenced read finds no holder and fences every queue: an ask meanwhile is answered
/// `fenced` and parked, never numbered, on a connection the gateway keeps open. Once the install's
/// connection has closed and its fence is lifted, the parked ask is numbered on that same
/// connection and told it holds the turn: no restart, and no ask again (05_gateway.md "The read
/// fences the turns").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parked_ask_holds_its_turn_once_the_fence_lifts_without_a_restart() {
    let (_gateway, _root, socket) = gateway().await;
    let (read, mut install) = fenced_read(&socket).await;
    let mut land = ask_raw(&socket, "land").await;
    let answer = raw_answer(&mut land).await;
    let listed = tickets(&socket).await;
    assert_eq!(
        (read, answer, listed),
        (
            serde_json::json!({"ok": true, "landTurns": [], "fenced": true}),
            serde_json::json!({"turn": "fenced"}),
            Vec::<String>::new(),
        ),
        "the install's read, the land's answer and the tickets listed under the fence"
    );
    // The install's release: its end of file, then the gateway's once the fence is lifted.
    install.shutdown().await.expect("end the install's read");
    let mut rest = Vec::new();
    awaiting("the fence's release", install.read_to_end(&mut rest))
        .await
        .expect("read to the gateway's end of file");
    assert!(
        rest.is_empty(),
        "the gateway sent more after its answer: {rest:?}"
    );
    let held = raw_answer(&mut land).await;
    assert_eq!(
        (
            &held["turn"],
            &held["ticket"]["number"],
            &held["ticket"]["workspace"]
        ),
        (
            &serde_json::json!("held"),
            &serde_json::json!(1),
            &serde_json::json!("land")
        ),
        "the parked ask is numbered first on its own connection: {held}"
    );
}

/// A fenced read that finds a holder sets no fence: the install refuses, and a land asking after
/// it queues as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fenced_read_that_finds_a_holder_sets_no_fence() {
    let (_gateway, _root, socket) = gateway().await;
    let first = take_free(&socket, "first").await;
    let (read, _install) = fenced_read(&socket).await;
    assert_eq!(read["fenced"], serde_json::json!(false), "{read}");
    let mut second = ask_raw(&socket, "second").await;
    assert_eq!(raw_answer(&mut second).await["turn"], "queued");
    first.end("exited 0".to_owned());
    assert_eq!(raw_answer(&mut second).await["turn"], "held");
}

/// An install's wait for the land turns, asked as `tooling/land-install.sh` runs `setup` under
/// `cowshed gateway await-turns`: the connection that holds its place, and then its fence, for as
/// long as it is open.
async fn waiting_fence(socket: &Path) -> Lines<BufReader<UnixStream>> {
    let mut install = UnixStream::connect_bounded(socket)
        .await
        .expect("connect the install's wait");
    let wait = serde_json::json!({
        "op": land_turn::LAND_TURNS_OP,
        "abandonedSinceUnixMs": u64::MAX,
        "fence": "wait",
    });
    install
        .write_all(format!("{wait}\n").as_bytes())
        .await
        .expect("send the install's wait");
    BufReader::new(install).lines()
}

/// The next control answer on an install's wait, past the heartbeats the gateway writes while the
/// install waits.
async fn control_line(answers: &mut Lines<BufReader<UnixStream>>) -> serde_json::Value {
    loop {
        let line = awaiting("the gateway's next control answer", answers.next_line())
            .await
            .expect("read the gateway's control answer")
            .expect("the gateway answers");
        let answer: serde_json::Value = serde_json::from_str(&line).expect("a control answer");
        if answer.get("progress").is_none() {
            return answer;
        }
    }
}

/// An install that finds a land's turn held waits for it in order instead of deferring
/// (05_gateway.md "The read fences the turns"): a land that asks while it waits is parked and never
/// numbered, so the install is fenced the moment the ticket queued before it ends, and the parked
/// land is numbered once the install's fence lifts. It runs after the install, never before it.
/// Recorded 2026-11-10: land 124380eca9's after-land install deferred at 12:16, 12:48 and
/// 13:17Z, each rerun losing the gap to the next land's ask.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_land_that_asks_after_a_waiting_install_runs_after_it() {
    let (_gateway, _root, socket) = gateway().await;
    let first = take_free(&socket, "first").await;
    let mut install = waiting_fence(&socket).await;
    let waiting = control_line(&mut install).await;
    assert_eq!(
        (
            &waiting["ok"],
            &waiting["fenced"],
            &waiting["landTurns"][0]["tickets"][0]["workspace"]
        ),
        (
            &serde_json::json!(true),
            &serde_json::json!(false),
            &serde_json::json!("first")
        ),
        "the install is told it waits, and for whom: {waiting}"
    );
    let mut later = ask_raw(&socket, "later").await;
    assert_eq!(
        raw_answer(&mut later).await,
        serde_json::json!({"turn": "fenced"})
    );
    assert_eq!(
        tickets(&socket).await,
        ["first"],
        "an ask while an install waits is parked, never numbered"
    );
    first.end("exited 0".to_owned());
    let fenced = control_line(&mut install).await;
    assert_eq!(
        (&fenced["ok"], &fenced["fenced"]),
        (&serde_json::json!(true), &serde_json::json!(true)),
        "the install is fenced once the ticket queued before it has ended: {fenced}"
    );
    assert_eq!(
        tickets(&socket).await,
        Vec::<String>::new(),
        "the parked land is not numbered under the install's fence"
    );
    // The install's command ends: its connection closes, and the gateway lifts the fence.
    let mut install = install.into_inner().into_inner();
    install.shutdown().await.expect("end the install's wait");
    let mut rest = Vec::new();
    awaiting("the fence's release", install.read_to_end(&mut rest))
        .await
        .expect("read to the gateway's end of file");
    assert!(
        rest.is_empty(),
        "the gateway sent more after the fence: {rest:?}"
    );
    let held = raw_answer(&mut later).await;
    assert_eq!(
        (&held["turn"], &held["ticket"]["workspace"]),
        (&serde_json::json!("held"), &serde_json::json!("later")),
        "the parked land holds the turn once the install's fence lifts: {held}"
    );
}

/// A gateway stand-in at `socket`: each connection it accepts is said on the returned channel by
/// its order, from 1, after the ask it carries has been read and the answers `script` gives for
/// that order have been written. `script` answers `None` once the stand-in has nothing more to
/// say: the connection is then held open until `close` gives the go for it, or the test ends.
fn stand_in(
    socket: &Path,
    script: impl Fn(usize) -> Vec<serde_json::Value> + Send + 'static,
    mut close: tokio::sync::mpsc::UnboundedReceiver<usize>,
    closes: impl Fn(usize) -> bool + Send + 'static,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<usize>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::UnixListener::bind(socket).expect("bind the stand-in gateway");
    let (accepted, told) = tokio::sync::mpsc::unbounded_channel();
    let serving = tokio::spawn(async move {
        let mut open = Vec::new();
        let mut order = 0;
        loop {
            order += 1;
            let (stream, _) = listener.accept().await.expect("accept a land");
            let (read, mut write) = stream.into_split();
            let mut asks = BufReader::new(read).lines();
            asks.next_line()
                .await
                .expect("read the land's ask")
                .expect("the land asks");
            for answer in script(order) {
                write
                    .write_all(format!("{answer}\n").as_bytes())
                    .await
                    .expect("answer the land");
            }
            // A test that has stopped reading has what it needed of the stand-in.
            let _ = accepted.send(order);
            if closes(order) {
                while close.recv().await.is_some_and(|go| go != order) {}
                continue;
            }
            open.push((asks, write));
        }
    });
    (told, serving)
}

/// A ticket the stand-in gateway hands the turn to.
fn held_by_stand_in(number: u64, workspace: &str) -> serde_json::Value {
    serde_json::json!({
        "turn": "held",
        "ticket": {
            "number": number,
            "pid": 1,
            "workspace": workspace,
            "askedUnixMs": 0,
            "command": format!("land {workspace}"),
        },
    })
}

/// A parked ask is answered on the connection it asked on (05_gateway.md "The read fences the
/// turns"): the asker that hears `fenced` says it waits for the install and keeps reading, and
/// hears its turn there once the gateway numbers it. It never asks again: the stand-in refuses a
/// second ask.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parked_asker_hears_its_turn_on_the_connection_it_asked_on() {
    let id = Uuid::new_v4().simple().to_string();
    let root = fixture_dir::secure_fixture_dir(&format!("csturn-{}", &id[..8]));
    let socket = root.path().join("gateway.sock");
    let gateway = turn_gateway(&socket);
    std::fs::create_dir(&gateway.support).expect("support");
    let (_close, closing) = tokio::sync::mpsc::unbounded_channel();
    let (mut accepted, serving) = stand_in(
        &socket,
        |order| match order {
            1 => vec![
                serde_json::json!({"turn": "fenced"}),
                held_by_stand_in(7, "land"),
            ],
            _ => vec![serde_json::json!({
                "turn": "refused",
                "error": "asked again: a parked ask is answered on the connection it asked on",
            })],
        },
        closing,
        |_| false,
    );
    let (told, mut waits) = tokio::sync::mpsc::unbounded_channel();
    let turn = awaiting(
        "the parked land's turn",
        land_turn::take(
            &gateway,
            &ask("land"),
            None,
            Duration::from_secs(90),
            move |wait| {
                // A test that has stopped reading the waits has what it needed of them.
                let _ = told.send(wait.clone());
            },
        ),
    )
    .await
    .expect("the parked ask is numbered on the connection it asked on");
    assert_eq!(turn.ticket().number, 7);
    assert_eq!(accepted.try_recv(), Ok(1));
    assert_eq!(
        accepted.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty),
        "the land asked again"
    );
    assert_eq!(
        waits.try_recv(),
        Ok(Wait::Install {
            lock: gateway.slot()
        })
    );
    serving.abort();
}

/// A parked ask whose connection the gateway closes, as a restart's drain does, waits for the
/// install holding the slot and then asks the gateway again; a fence met again is waited for the
/// same way, never refused, since only an install's open connection holds one (05_gateway.md "The
/// read fences the turns").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parked_ask_closed_by_the_gateway_waits_for_the_install_and_asks_again() {
    let id = Uuid::new_v4().simple().to_string();
    let root = fixture_dir::secure_fixture_dir(&format!("csturn-{}", &id[..8]));
    let socket = root.path().join("gateway.sock");
    let gateway = turn_gateway(&socket);
    std::fs::create_dir(&gateway.support).expect("support");
    let (close, closing) = tokio::sync::mpsc::unbounded_channel();
    let (mut accepted, serving) = stand_in(
        &socket,
        |order| match order {
            1 | 2 => vec![serde_json::json!({"turn": "fenced"})],
            _ => vec![held_by_stand_in(9, "land")],
        },
        closing,
        |order| order <= 2,
    );
    let land = tokio::spawn({
        let gateway = gateway.clone();
        async move {
            land_turn::take(
                &gateway,
                &ask("land"),
                None,
                Duration::from_secs(90),
                |_| {},
            )
            .await
        }
    });
    // Parked, then closed with no install holding the slot: the land asks again at once.
    assert_eq!(awaiting("the first ask", accepted.recv()).await, Some(1));
    close.send(1).expect("close the first ask");
    assert_eq!(awaiting("the second ask", accepted.recv()).await, Some(2));
    // Parked again; an install holds the slot as the gateway closes the ask.
    let slot = std::fs::File::create(gateway.slot()).expect("the install slot");
    slot.try_lock().expect("the install takes the free slot");
    close.send(2).expect("close the second ask");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), accepted.recv())
            .await
            .is_err(),
        "the land asked again while the install held the slot"
    );
    drop(slot);
    assert_eq!(awaiting("the third ask", accepted.recv()).await, Some(3));
    let turn = awaiting("the land's turn", land)
        .await
        .expect("the land task")
        .expect("a fence met again is waited for, never refused");
    assert_eq!(turn.ticket().number, 9);
    serving.abort();
}

/// A connection the gateway closes before any answer is no fence: the ask fails, naming the close.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ask_closed_before_any_answer_fails() {
    let id = Uuid::new_v4().simple().to_string();
    let root = fixture_dir::secure_fixture_dir(&format!("csturn-{}", &id[..8]));
    let socket = root.path().join("gateway.sock");
    let gateway = turn_gateway(&socket);
    std::fs::create_dir(&gateway.support).expect("support");
    let (close, closing) = tokio::sync::mpsc::unbounded_channel();
    let (_accepted, serving) = stand_in(&socket, |_| Vec::new(), closing, |_| true);
    close.send(1).expect("close the ask unanswered");
    let refused = awaiting(
        "the land's refusal",
        land_turn::take(
            &gateway,
            &ask("land"),
            None,
            Duration::from_secs(90),
            |_| {},
        ),
    )
    .await
    .expect_err("an unanswered close is no turn");
    assert!(
        refused
            .to_string()
            .contains("closed the land turn's connection"),
        "{refused}"
    );
    serving.abort();
}
//#endregion smoo/cowshed!n/read-fences-turns

/// Recorded 2026-10-28 13:52:39Z (`lands/8e24efb3….after2.log:77`): a land passed its slot check,
/// an install took the slot and read no turn holder, the land asked its turn and bound its gate,
/// and the install restarted the gateway, whose drain closed the turn's connection: the land lost
/// its turn and sent its gate SIGTERM. The install's read fences the turns, so the land's ask is
/// answered `fenced` and waits for the slot; the install restarts the gateway and lets the slot go
/// once the new one serves, and the land takes its turn of the new gateway and binds its gate,
/// which no restart reaches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_recorded_install_restart_never_cuts_a_turn_asked_after_its_read() {
    let (old, root, socket) = gateway().await;
    let gateway = turn_gateway(&socket);
    std::fs::create_dir(&gateway.support).expect("support");
    let slot = std::fs::File::create(gateway.slot()).expect("the install slot");
    slot.try_lock().expect("the install takes the free slot");
    let (read, install) = fenced_read(&socket).await;
    assert_eq!(
        read["fenced"],
        serde_json::json!(true),
        "no holder, so the read fences: {read}"
    );
    let (told, mut waits) = tokio::sync::mpsc::unbounded_channel();
    let land = tokio::spawn({
        let gateway = gateway.clone();
        async move {
            land_turn::take(
                &gateway,
                &ask("land"),
                None,
                Duration::from_secs(90),
                move |wait| {
                    // A test that has stopped reading the waits has what it needed of them.
                    let _ = told.send(wait.clone());
                },
            )
            .await
            .expect("the land's turn")
        }
    });
    assert_eq!(
        awaiting("the land told the install's fence holds it", waits.recv()).await,
        Some(Wait::Install {
            lock: gateway.slot()
        })
    );
    assert_eq!(
        tickets(&socket).await,
        Vec::<String>::new(),
        "a fenced ask is never numbered"
    );
    // The install restarts the gateway: the old one drains, closing the read's connection, and
    // the install lets the slot go once the new one serves.
    old.drain().await.expect("the old gateway drains");
    drop(install);
    let _new = gateway_at(&socket, &root.path().join("cache")).await;
    drop(slot);
    let mut turn = awaiting("the land's turn of the new gateway", land)
        .await
        .expect("the land task");
    let mut gate = tokio::process::Command::new("sh");
    gate.args(["-c", "exec cat >/dev/null"])
        .stdin(std::process::Stdio::piped())
        .kill_on_drop(true);
    turn.bind_at_exec(gate.as_std_mut());
    let mut gate = gate.spawn().expect("spawn the gate");
    awaiting(
        "the gate's bind",
        turn.bound(gate.id().expect("a running gate has a pid")),
    )
    .await
    .expect("bind the gate");
    drop(gate.stdin.take());
    tokio::select! {
        biased;
        lost = turn.lost() => panic!(
            "the restart cut the land's turn under its gate, which the land then sends SIGTERM: {lost}"
        ),
        ended = gate.wait_bounded() => assert!(ended.expect("the gate ends").success()),
    }
    turn.end("exited 0".to_owned());
}
