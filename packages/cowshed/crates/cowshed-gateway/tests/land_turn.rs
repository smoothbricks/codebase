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
        class: None,
        resumable: false,
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
    first.end("exited 0".to_owned()).expect("end first");
    let second = awaiting("second's turn", second)
        .await
        .expect("second's turn");
    assert!(!third.is_finished(), "third took the turn ahead of second");
    second.end("exited 0".to_owned()).expect("end second");
    awaiting("third's turn", third)
        .await
        .expect("third's turn")
        .end("exited 0".to_owned())
        .expect("end third");
    let probe = take_after(&socket, "probe").await;
    let listed = queue(&socket).await.expect("the probe's queue is listed");
    assert!(
        listed.abandoned.is_empty(),
        "an ended turn is no abandonment: {:?}",
        listed.abandoned
    );
    probe.end("exited 0".to_owned()).expect("end probe");
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
    first.end("exited 0".to_owned()).expect("end first");
    // Second holds the turn once first's departure is processed, the descendant still running.
    let second = take_after(&socket, "second").await;
    // SAFETY: kill(2) with signal 0 only checks that the process exists.
    assert_eq!(
        unsafe { libc::kill(descendant, 0) },
        0,
        "the descendant still runs"
    );
    second.end("exited 0".to_owned()).expect("end second");
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
    first
        .end("its command runs on".to_owned())
        .expect("end first");
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
        .end("exited 0".to_owned())
        .expect("end second");
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
        .end("exited 0".to_owned())
        .expect("end second");
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
    first.end("exited 0".to_owned()).expect("end first");
    awaiting("third's turn", third)
        .await
        .expect("third's turn")
        .end("exited 0".to_owned())
        .expect("end third");
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
    probe.end("exited 0".to_owned()).expect("end probe");
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
    writer.end("committed".to_owned()).expect("end writer");
    awaiting("the land's ref turn", land)
        .await
        .expect("the land task")
        .expect("the land takes the released ref turn")
        .end("delivered".to_owned())
        .expect("end land ref turn");
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

/// A bounded ask (`"class":"bounded"`) from outside every held turn is numbered at once while an
/// install waits if some queue still holds a non-bounded ticket, takes its turn, and completes
/// while the install still waits (05_gateway.md "The read fences the turns").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bounded_ask_behind_a_held_land_is_numbered_while_an_install_waits() {
    let (_gateway, _root, socket) = gateway().await;
    // Hold the land turn from a separate unfenced legacy process so later asks from this process
    // are from outside and the install genuinely waits for the holder to end.
    let land = LegacyHolder::start(&socket, REPO, "land", false).await;

    let mut install = waiting_fence(&socket).await;
    let waiting = control_line(&mut install).await;
    assert_eq!(waiting["fenced"], serde_json::json!(false), "{waiting}");

    // A bounded ask on `<REPO>.ref` arrives while the land holds `<REPO>` and the install waits.
    let mut doc = UnixStream::connect_bounded(&socket)
        .await
        .expect("connect bounded ask");
    let ask = serde_json::json!({
        "op": land_turn::LAND_TURN_OP,
        "repo": format!("{REPO}.ref"),
        "workspace": "doc-commit",
        "command": "cowshed doc-commit",
        "class": "bounded",
    });
    doc.write_all(format!("{ask}\n").as_bytes())
        .await
        .expect("send bounded ask");
    let (doc_read, mut doc_write) = doc.into_split();
    let mut doc_answers = BufReader::new(doc_read).lines();
    let first = raw_answer(&mut doc_answers).await;
    assert_eq!(
        (&first["turn"], &first["ticket"]["workspace"]),
        (&serde_json::json!("held"), &serde_json::json!("doc-commit")),
        "bounded ask behind a held land must be numbered immediately, not parked: {first}"
    );
    doc_write
        .write_all(b"{\"step\":\"end\",\"outcome\":\"committed\"}\n")
        .await
        .expect("end bounded turn");
    drop(doc_write);
    drop(doc_answers);

    // Release the land: now the install is fenced.
    land.finish().await;
    let fenced = control_line(&mut install).await;
    assert_eq!(fenced["fenced"], serde_json::json!(true), "{fenced}");
}

/// Once only bounded tickets remain in the queues while an install waits, a new bounded ask from
/// outside parks (`{"turn":"fenced"}`) until the install's fence lifts, so the install waits at
/// most one bound past the last land ticket (05_gateway.md "The read fences the turns").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bounded_ask_parks_once_only_bounded_tickets_remain() {
    let (_gateway, _root, socket) = gateway().await;
    // First bounded ticket holds `<REPO>.ref` from a separate unfenced process so later asks are
    // from outside and the install genuinely waits for it to end.
    let first = LegacyHolder::start(&socket, &format!("{REPO}.ref"), "doc-first", true).await;

    // Install waits: only the bounded ticket is in any queue.
    let mut install = waiting_fence(&socket).await;
    let waiting = control_line(&mut install).await;
    assert_eq!(waiting["fenced"], serde_json::json!(false), "{waiting}");

    // Second bounded ask arrives with no non-bounded ticket left: it must park.
    let mut second = UnixStream::connect_bounded(&socket)
        .await
        .expect("connect second bounded ask");
    let second_ask = serde_json::json!({
        "op": land_turn::LAND_TURN_OP,
        "repo": format!("{REPO}.ref"),
        "workspace": "doc-second",
        "command": "cowshed doc-commit",
        "class": "bounded",
    });
    second
        .write_all(format!("{second_ask}\n").as_bytes())
        .await
        .expect("send second bounded ask");
    let mut second_answers = BufReader::new(second).lines();
    assert_eq!(
        raw_answer(&mut second_answers).await,
        serde_json::json!({"turn": "fenced"}),
        "bounded ask must park once only bounded tickets remain"
    );

    // First bounded ticket ends -> install is fenced -> install lifts -> second is numbered.
    first.finish().await;
    let fenced = control_line(&mut install).await;
    assert_eq!(fenced["fenced"], serde_json::json!(true), "{fenced}");
    drop(install);
    let second_held = raw_answer(&mut second_answers).await;
    assert_eq!(
        (&second_held["turn"], &second_held["ticket"]["workspace"]),
        (&serde_json::json!("held"), &serde_json::json!("doc-second")),
        "parked bounded ask is numbered once install's fence lifts: {second_held}"
    );
}

/// A bounded ticket still held at `bounded_turn_deadline` is ended by the gateway with
/// `{"turn":"expired","boundMs":...}` and a closed connection (05_gateway.md "Deadlines").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bounded_ticket_past_its_deadline_is_ended_and_names_the_bound() {
    let id = Uuid::new_v4().simple().to_string();
    let root = fixture_dir::secure_fixture_dir(&format!("csturn-{}", &id[..8]));
    let cache = root.path().join("cache");
    std::fs::create_dir(&cache).expect("cache");
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).expect("secure cache");
    let socket = root.path().join("gateway.sock");
    let bound = Duration::from_millis(200);
    let _gateway = Gateway::start(
        GatewayConfig {
            control_socket: Some(socket.clone()),
            mirror_cache: MirrorCacheConfig::new(cache),
            bounded_turn_deadline: bound,
            ..GatewayConfig::default()
        },
        Arc::new(NoCredentials),
        Arc::new(NoConnector),
        Arc::new(DiscardAudit),
    )
    .await
    .expect("start gateway with short bounded-turn deadline");

    let mut doc = UnixStream::connect_bounded(&socket)
        .await
        .expect("connect bounded ask");
    let ask = serde_json::json!({
        "op": land_turn::LAND_TURN_OP,
        "repo": format!("{REPO}.ref"),
        "workspace": "doc-commit",
        "command": "cowshed doc-commit",
        "class": "bounded",
    });
    doc.write_all(format!("{ask}\n").as_bytes())
        .await
        .expect("send bounded ask");
    let mut answers = BufReader::new(doc).lines();
    let held = raw_answer(&mut answers).await;
    assert_eq!(held["turn"], serde_json::json!("held"), "{held}");

    let expired = raw_answer(&mut answers).await;
    assert_eq!(
        (&expired["turn"], &expired["boundMs"]),
        (&serde_json::json!("expired"), &serde_json::json!(200)),
        "gateway must expire the bounded ticket at the bound: {expired}"
    );
    let eof = awaiting("connection close after expiry", answers.next_line())
        .await
        .expect("read eof");
    assert_eq!(eof, None, "gateway closes the expired turn's connection");
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
async fn raw_answer<R: tokio::io::AsyncBufRead + Unpin>(
    answers: &mut Lines<R>,
) -> serde_json::Value {
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
    first.end("exited 0".to_owned()).expect("end first");
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
    // Hold the first turn from a separate unfenced legacy process so later asks from this process
    // are from outside and the install waits for it to end.
    let first = LegacyHolder::start(&socket, REPO, "first", false).await;
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
    first.finish().await;
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

/// A land takes its ref turn inside its land turn though an install waits for that land turn
/// (05_gateway.md "The read fences the turns"): the install waits for the land's ticket, which ends
/// only after the fast-forward under the ref turn, so the ask made inside the held turn is numbered,
/// never parked behind the install. The land then ends both turns, and that step fences the
/// install. Recorded 2026-11-16: the ref ask was parked behind the install of the land before, and
/// the land and that install each waited for the other for 24 minutes, until the land was ended by
/// hand.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_land_takes_its_ref_turn_inside_its_turn_though_an_install_waits_for_that_turn() {
    let (_gateway, _root, socket) = gateway().await;
    let land = take_free(&socket, "second").await;
    let mut install = waiting_fence(&socket).await;
    let waiting = control_line(&mut install).await;
    assert_eq!(
        (
            &waiting["fenced"],
            &waiting["landTurns"][0]["tickets"][0]["workspace"]
        ),
        (&serde_json::json!(false), &serde_json::json!("second")),
        "the install waits for the land's turn: {waiting}"
    );
    // The land's gate has passed: it asks its ref turn, and is told at once that it holds it.
    let fence = take_asked(
        &socket,
        &TurnAsk {
            repo: LandTurnName::new("acme.ref").unwrap(),
            workspace: "second".to_owned(),
            command: "cowshed land second (ref fence)".to_owned(),
            class: None,
            resumable: false,
        },
    )
    .await;
    fence.end("delivered".to_owned()).expect("end fence");
    land.end("exited 0".to_owned()).expect("end land");
    let fenced = control_line(&mut install).await;
    assert_eq!(
        (&fenced["ok"], &fenced["fenced"], &fenced["landTurns"]),
        (
            &serde_json::json!(true),
            &serde_json::json!(true),
            &serde_json::json!([])
        ),
        "the install is fenced once the land has ended both turns: {fenced}"
    );
}

/// Withdrawing a waiting install via `{"op":"land-turns-withdraw"}` leaves its wait (`TWaitLeave`),
/// tells the waiting install it was withdrawn by the withdrawer's pid, and admits every parked ask
/// in arrival order at once without disturbing the held turn (05_gateway.md "The read fences the
/// turns").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiting_install_withdrawn_leaves_the_wait_and_admits_parked_asks_in_order() {
    let (_gateway, _root, socket) = gateway().await;
    // Hold the turn from a separate unfenced legacy process so later asks from this process are
    // from outside and the install waits for it to end.
    let first = LegacyHolder::start(&socket, REPO, "first", false).await;

    let mut install = waiting_fence(&socket).await;
    let waiting = control_line(&mut install).await;
    assert_eq!(
        (&waiting["ok"], &waiting["fenced"]),
        (&serde_json::json!(true), &serde_json::json!(false)),
        "the install waits behind the held turn: {waiting}"
    );

    let mut parked_a = ask_raw(&socket, "parked-a").await;
    assert_eq!(
        raw_answer(&mut parked_a).await,
        serde_json::json!({"turn": "fenced"})
    );
    let mut parked_b = ask_raw(&socket, "parked-b").await;
    assert_eq!(
        raw_answer(&mut parked_b).await,
        serde_json::json!({"turn": "fenced"})
    );
    assert_eq!(tickets(&socket).await, ["first"]);

    let mut withdraw = UnixStream::connect_bounded(&socket)
        .await
        .expect("connect to withdraw the waiting install");
    let request = serde_json::json!({"op": "land-turns-withdraw"});
    withdraw
        .write_all(format!("{request}\n").as_bytes())
        .await
        .expect("send land-turns-withdraw");
    withdraw.shutdown().await.expect("half-close withdraw");
    let mut reply_line = String::new();
    awaiting(
        "the withdraw reply",
        BufReader::new(&mut withdraw).read_line(&mut reply_line),
    )
    .await
    .expect("read withdraw reply");
    let reply: serde_json::Value = serde_json::from_str(&reply_line).expect("parse withdraw reply");
    assert_eq!(
        reply["ok"],
        serde_json::json!(true),
        "withdraw reply: {reply}"
    );
    let withdrawn = reply["withdrawnInstalls"]
        .as_array()
        .expect("withdrawnInstalls array");
    assert_eq!(withdrawn.len(), 1, "one install was waiting: {reply}");
    assert_eq!(
        withdrawn[0]["pid"],
        serde_json::json!(std::process::id()),
        "withdrawn install names its pid: {reply}"
    );
    assert!(
        withdrawn[0]["waitedMs"].is_u64(),
        "withdrawn install names how long it waited: {reply}"
    );

    let install_reply = control_line(&mut install).await;
    assert_eq!(
        (
            &install_reply["ok"],
            &install_reply["withdrawn"],
            &install_reply["withdrawnByPid"],
        ),
        (
            &serde_json::json!(true),
            &serde_json::json!(true),
            &serde_json::json!(std::process::id()),
        ),
        "the waiting install is told it was withdrawn and by whom: {install_reply}"
    );

    let queued_a = raw_answer(&mut parked_a).await;
    let queued_b = raw_answer(&mut parked_b).await;
    assert_eq!(
        (
            &queued_a["turn"],
            &queued_a["ticket"]["workspace"],
            &queued_a["ahead"],
        ),
        (
            &serde_json::json!("queued"),
            &serde_json::json!("parked-a"),
            &serde_json::json!(1),
        ),
        "first parked ask is numbered first behind the holder: {queued_a}"
    );
    assert_eq!(
        (
            &queued_b["turn"],
            &queued_b["ticket"]["workspace"],
            &queued_b["ahead"],
        ),
        (
            &serde_json::json!("queued"),
            &serde_json::json!("parked-b"),
            &serde_json::json!(2),
        ),
        "second parked ask is numbered second behind the holder: {queued_b}"
    );
    assert!(
        queued_a["ticket"]["number"].as_u64() < queued_b["ticket"]["number"].as_u64(),
        "parked asks enter in arrival order: {queued_a} vs {queued_b}"
    );
    assert_eq!(tickets(&socket).await, ["first", "parked-a", "parked-b"]);

    first.finish().await;
}

/// An unfenced `land-turns` read (what `cowshed gateway status` sends) reports every waiting
/// install and every parked ask from the actor's own state without setting a fence
/// (05_gateway.md:392, 700-704).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn land_turns_lists_waiting_installs_and_parked_asks_without_fencing() {
    let (_gateway, _root, socket) = gateway().await;
    let holder = LegacyHolder::start(&socket, REPO, "holder", false).await;

    let mut install = waiting_fence(&socket).await;
    let waiting = control_line(&mut install).await;
    assert_eq!(waiting["fenced"], serde_json::json!(false), "{waiting}");

    let mut parked = ask_raw(&socket, "parked-land").await;
    assert_eq!(
        raw_answer(&mut parked).await,
        serde_json::json!({"turn": "fenced"})
    );

    let mut status = UnixStream::connect_bounded(&socket)
        .await
        .expect("connect to read land-turns");
    let request = serde_json::json!({
        "op": land_turn::LAND_TURNS_OP,
        "abandonedSinceUnixMs": u64::MAX,
    });
    status
        .write_all(format!("{request}\n").as_bytes())
        .await
        .expect("send unfenced land-turns");
    status.shutdown().await.expect("half-close land-turns");
    let mut line = String::new();
    awaiting(
        "the land-turns answer",
        BufReader::new(&mut status).read_line(&mut line),
    )
    .await
    .expect("read land-turns");
    let answer: serde_json::Value = serde_json::from_str(&line).expect("parse land-turns");
    assert_eq!(answer["ok"], serde_json::json!(true), "{answer}");
    assert_eq!(
        answer["landTurns"][0]["tickets"][0]["workspace"],
        serde_json::json!("holder"),
        "{answer}"
    );

    let waiting_installs = answer["waitingInstalls"]
        .as_array()
        .expect("waitingInstalls array in land-turns answer");
    assert_eq!(waiting_installs.len(), 1, "{answer}");
    assert_eq!(
        waiting_installs[0]["pid"],
        serde_json::json!(std::process::id()),
        "{answer}"
    );
    assert!(waiting_installs[0]["waitingUnixMs"].is_u64(), "{answer}");

    let parked_asks = answer["parkedAsks"]
        .as_array()
        .expect("parkedAsks array in land-turns answer");
    assert_eq!(parked_asks.len(), 1, "{answer}");
    assert_eq!(
        (
            &parked_asks[0]["repo"],
            &parked_asks[0]["workspace"],
            &parked_asks[0]["pid"],
            &parked_asks[0]["command"],
        ),
        (
            &serde_json::json!(REPO),
            &serde_json::json!("parked-land"),
            &serde_json::json!(std::process::id()),
            &serde_json::json!("land parked-land"),
        ),
        "{answer}"
    );
    assert!(parked_asks[0]["askedUnixMs"].is_u64(), "{answer}");

    // The status read set no fence: closing the waiting install immediately admits the parked ask.
    drop(install);
    let queued = raw_answer(&mut parked).await;
    assert_eq!(
        (&queued["turn"], &queued["ticket"]["workspace"]),
        (
            &serde_json::json!("queued"),
            &serde_json::json!("parked-land")
        ),
        "unfenced status read must not keep asks parked: {queued}"
    );

    holder.finish().await;
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
    turn.end("exited 0".to_owned()).expect("end turn");
}

/// Connect raw to `socket` and ask for a resumable land turn (`"resumable": true`), returning the
/// split line reader and writer.
async fn ask_resumable_raw(
    socket: &Path,
    repo: &str,
    workspace: &str,
) -> (
    Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    tokio::net::unix::OwnedWriteHalf,
) {
    let mut stream = UnixStream::connect_bounded(socket)
        .await
        .expect("connect resumable ask");
    let request = serde_json::json!({
        "op": land_turn::LAND_TURN_OP,
        "repo": repo,
        "workspace": workspace,
        "command": format!("land {workspace}"),
        "resumable": true,
    });
    stream
        .write_all(format!("{request}\n").as_bytes())
        .await
        .expect("send resumable ask");
    let (read, write) = stream.into_split();
    (BufReader::new(read).lines(), write)
}

/// Send `{"op":"land-turn-resume","ticket":...,"token":...}` on a fresh connection to `socket`.
async fn resume_raw(
    socket: &Path,
    ticket: u64,
    token: &str,
) -> (
    Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    tokio::net::unix::OwnedWriteHalf,
) {
    let mut stream = UnixStream::connect_bounded(socket)
        .await
        .expect("connect land-turn-resume");
    let request = serde_json::json!({
        "op": land_turn::LAND_TURN_RESUME_OP,
        "ticket": ticket,
        "token": token,
    });
    stream
        .write_all(format!("{request}\n").as_bytes())
        .await
        .expect("send land-turn-resume");
    let (read, write) = stream.into_split();
    (BufReader::new(read).lines(), write)
}

/// A separate OS process holding an unfenced legacy turn (no `"resumable":true`) so asks from the
/// test process come from outside the held turn and a waiting install genuinely waits for it to
/// end.
struct LegacyHolder {
    child: tokio::process::Child,
    stdin: Option<tokio::process::ChildStdin>,
}

impl LegacyHolder {
    async fn start(socket: &Path, repo: &str, workspace: &str, bounded: bool) -> Self {
        let script = r#"
import json, socket, sys

sock_path, repo, ws, bounded = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] == "1"
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(sock_path)
ask = {
    "op": "land-turn",
    "repo": repo,
    "workspace": ws,
    "command": f"land {ws}",
}
if bounded:
    ask["class"] = "bounded"
s.sendall((json.dumps(ask) + "\n").encode())
f = s.makefile("r", encoding="utf-8")
first = f.readline()
sys.stdout.write(first)
sys.stdout.flush()
sys.stdin.readline()
s.sendall(b'{"step":"end","outcome":"exited 0"}\n')
try:
    s.shutdown(socket.SHUT_WR)
except OSError:
    pass
f.read()
"#;
        let mut cmd = tokio::process::Command::new("python3");
        cmd.args([
            "-c",
            script,
            socket.to_str().expect("utf8 socket"),
            repo,
            workspace,
            if bounded { "1" } else { "0" },
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true);
        let mut child = cmd
            .spawn()
            .expect("spawn legacy holder (python3, which the development shell provides)");
        let stdin = child.stdin.take();
        let mut out = BufReader::new(child.stdout.take().expect("legacy holder stdout")).lines();
        let held = raw_answer(&mut out).await;
        assert_eq!(held["turn"], serde_json::json!("held"), "{held}");
        Self { child, stdin }
    }

    async fn finish(mut self) {
        drop(self.stdin.take());
        let status = self
            .child
            .wait_bounded()
            .await
            .expect("wait for legacy holder");
        assert!(status.success(), "legacy holder exited {status}");
    }
}

/// Spawn a separate OS process that connects to `socket`, asks for a resumable turn on `REPO`,
/// optionally spawns and binds a child `sh -c "exec cat >/dev/null"` (reporting its pid), prints
/// each received JSON line to stdout, and forwards any stdin lines straight onto the socket.
/// Dropping or SIGKILLing this child simulates an asker crashing during the restart gap.
fn spawn_remote_asker(socket: &Path, workspace: &str, bind_child: bool) -> tokio::process::Child {
    let script = r#"
import json, os, socket, subprocess, sys, threading

sock_path, repo, ws, bind_flag = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] == "1"
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(sock_path)
s.sendall((json.dumps({
    "op": "land-turn",
    "repo": repo,
    "workspace": ws,
    "command": f"land {ws}",
    "resumable": True,
}) + "\n").encode())
f = s.makefile("r", encoding="utf-8")
first = f.readline()
sys.stdout.write(first)
sys.stdout.flush()
msg = json.loads(first)
child = None
if bind_flag and msg.get("turn") == "held":
    child = subprocess.Popen(
        ["sh", "-c", "exec sleep 300"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    sys.stdout.write(json.dumps({"spawnedPid": child.pid}) + "\n")
    sys.stdout.flush()
    s.sendall((json.dumps({"step": "bind", "pid": child.pid}) + "\n").encode())
    bound = f.readline()
    sys.stdout.write(bound)
    sys.stdout.flush()

def pump_stdin():
    for line in sys.stdin:
        s.sendall(line.encode())
threading.Thread(target=pump_stdin, daemon=True).start()
for line in f:
    sys.stdout.write(line)
    sys.stdout.flush()
"#;
    let mut cmd = tokio::process::Command::new("python3");
    cmd.args([
        "-c",
        script,
        socket.to_str().expect("utf8 socket"),
        REPO,
        workspace,
        if bind_child { "1" } else { "0" },
    ])
    .stdin(std::process::Stdio::piped())
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::inherit())
    .kill_on_drop(true);
    cmd.spawn()
        .expect("spawn remote asker (python3, which the development shell provides)")
}

/// Option A happy path (05_gateway.md "The read fences the turns", "Land turns"): a held turn
/// mid-gate fences within a heartbeat on an install's wait, the install drains and restarts the
/// gateway at the same socket while the bound gate keeps running, the turn resumes with its token,
/// and a `RefTurn` (`acme.ref`) queued behind it acquires its turn and lands cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_turn_mid_gate_fences_within_a_heartbeat_survives_restart_and_a_queued_ref_turn_lands()
 {
    let (old, root, socket) = gateway().await;
    let cache = root.path().join("cache");
    let tg = turn_gateway(&socket);
    std::fs::create_dir(&tg.support).expect("support");

    let (mut land_r, mut land_w) = ask_resumable_raw(&socket, REPO, "mid-gate").await;
    let held = raw_answer(&mut land_r).await;
    assert_eq!(
        held["turn"],
        serde_json::json!("held"),
        "resumable ask must be answered held: {held}"
    );
    let ticket_num = held["ticket"]["number"].as_u64().expect("ticket number");
    let token = held["resumeToken"]
        .as_str()
        .expect("first numbered answer must carry a resumeToken")
        .to_owned();
    assert!(!token.is_empty(), "resumeToken must be non-empty");

    // Token must never appear in `land-turns` status listings.
    let raw_status = awaiting(
        "land-turns status",
        GatewayControlClient::new(socket.clone())
            .expect("client")
            .land_turns(u64::MAX),
    )
    .await
    .expect("land-turns");
    assert!(
        !serde_json::to_string(&raw_status)
            .expect("serialize status")
            .contains(&token),
        "resume token must never leak into gateway status"
    );

    // Spawn and bind a gate child that stays alive across the gateway restart.
    let mut gate = tokio::process::Command::new("sh");
    gate.args(["-c", "exec cat >/dev/null"])
        .stdin(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut gate = gate.spawn().expect("spawn gate");
    let gate_pid = gate.id().expect("gate pid");
    land_w
        .write_all(format!("{{\"step\":\"bind\",\"pid\":{gate_pid}}}\n").as_bytes())
        .await
        .expect("send bind");
    let bound = raw_answer(&mut land_r).await;
    assert_eq!(bound["turn"], serde_json::json!("bound"), "{bound}");

    // Install asks `fence: "wait"`: the gateway asks the resumable holder to fence, it replies
    // `{"step":"fenced"}`, and the install is fenced immediately while the gate keeps running.
    let mut install = waiting_fence(&socket).await;
    let initial = control_line(&mut install).await;
    assert_eq!(initial["fenced"], serde_json::json!(false), "{initial}");
    let fence_req = raw_answer(&mut land_r).await;
    assert_eq!(
        fence_req["turn"],
        serde_json::json!("fence"),
        "waiting install must ask resumable holder to fence: {fence_req}"
    );
    land_w
        .write_all(b"{\"step\":\"fenced\"}\n")
        .await
        .expect("ack fence");
    let fenced = control_line(&mut install).await;
    assert_eq!(
        fenced["fenced"],
        serde_json::json!(true),
        "install must be fenced once held turn acknowledges without waiting for gate: {fenced}"
    );

    // Real drain and restart of the gateway at the same socket under install.lock.
    let slot = std::fs::File::create(tg.slot()).expect("install slot");
    slot.try_lock().expect("take install slot");
    old.drain().await.expect("old gateway drains");
    drop(install);
    drop(land_r);
    drop(land_w);
    let new = gateway_at(&socket, &cache).await;
    drop(slot);

    assert!(
        gate.try_wait().expect("poll gate").is_none(),
        "bound gate must keep running untouched across gateway restart"
    );

    // Asker reconnects and resumes with its token.
    let (mut resumed_r, mut resumed_w) = resume_raw(&socket, ticket_num, &token).await;
    let resumed = raw_answer(&mut resumed_r).await;
    assert_eq!(
        (&resumed["turn"], &resumed["ticket"]["number"]),
        (
            &serde_json::json!("resumed"),
            &serde_json::json!(ticket_num)
        ),
        "replacing gateway must resume the adopted ticket: {resumed}"
    );

    // A doc-commit ref turn acquires `acme.ref` and finishes on the new gateway while the land
    // still holds `acme`.
    let ref_queue = LandTurnName::new("acme.ref").unwrap();
    let ref_turn = awaiting(
        "doc-commit ref turn on restarted gateway",
        RefTurn::acquire_in(&tg, &ref_queue, RefWriter::DocCommit),
    )
    .await
    .expect("doc-commit ref turn takes its turn after restart");
    ref_turn.end("committed".to_owned()).expect("end ref turn");

    // Gate finishes and land ends its turn cleanly.
    drop(gate.stdin.take());
    assert!(gate.wait_bounded().await.expect("gate exits").success());
    resumed_w
        .write_all(b"{\"step\":\"end\",\"outcome\":\"exited 0\"}\n")
        .await
        .expect("end resumed turn");
    drop(resumed_w);
    drop(resumed_r);
    let probe = take_after(&socket, "probe").await;
    assert!(
        queue(&socket).await.is_none_or(|q| q.abandoned.is_empty()),
        "resumed turn must finish cleanly, never abandoned"
    );
    probe.end("probed".to_owned()).expect("end probe turn");
    new.drain().await.expect("new gateway drains");
}

/// Adopting a handoff snapshot preserves both held and queued tickets in FIFO order, continues
/// ticket numbering past the previous counter, and unlinks the snapshot file after adoption
/// (05_gateway.md "Land turns").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adopting_a_snapshot_preserves_queue_order_and_ticket_numbering_and_settles_gone_processes()
{
    let (old, root, socket) = gateway().await;
    let cache = root.path().join("cache");

    let (mut first_r, mut first_w) = ask_resumable_raw(&socket, REPO, "first").await;
    let first_held = raw_answer(&mut first_r).await;
    assert_eq!(
        first_held["turn"],
        serde_json::json!("held"),
        "{first_held}"
    );
    let first_num = first_held["ticket"]["number"].as_u64().expect("first num");
    let first_token = first_held["resumeToken"]
        .as_str()
        .expect("first token")
        .to_owned();

    let (mut second_r, mut second_w) = ask_resumable_raw(&socket, REPO, "second").await;
    let second_queued = raw_answer(&mut second_r).await;
    assert_eq!(
        second_queued["turn"],
        serde_json::json!("queued"),
        "{second_queued}"
    );
    let second_num = second_queued["ticket"]["number"]
        .as_u64()
        .expect("second num");
    let second_token = second_queued["resumeToken"]
        .as_str()
        .expect("second token")
        .to_owned();

    let mut install = waiting_fence(&socket).await;
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(false)
    );
    assert_eq!(
        raw_answer(&mut first_r).await["turn"],
        serde_json::json!("fence")
    );
    assert_eq!(
        raw_answer(&mut second_r).await["turn"],
        serde_json::json!("fence")
    );
    first_w
        .write_all(b"{\"step\":\"fenced\"}\n")
        .await
        .expect("fence first");
    second_w
        .write_all(b"{\"step\":\"fenced\"}\n")
        .await
        .expect("fence second");
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(true)
    );

    old.drain().await.expect("drain old");
    drop(install);
    drop(first_r);
    drop(first_w);
    drop(second_r);
    drop(second_w);

    let new = gateway_at(&socket, &cache).await;
    let handoff = socket
        .parent()
        .expect("socket dir")
        .join("land-turns-handoff.json");
    assert!(
        !handoff.exists(),
        "handoff snapshot must be unlinked immediately after adoption"
    );
    assert_eq!(
        tickets(&socket).await,
        ["first", "second"],
        "queue order must survive restart"
    );

    // A new ask after restart must get a number strictly above `second_num`.
    let (mut third_r, third_w) = ask_resumable_raw(&socket, REPO, "third").await;
    let third_queued = raw_answer(&mut third_r).await;
    let third_num = third_queued["ticket"]["number"]
        .as_u64()
        .expect("third num");
    assert!(
        third_num > second_num,
        "numbering must continue past {second_num}, got {third_num}"
    );

    // Both adopted tickets resume and finish in order.
    let (mut r1, mut w1) = resume_raw(&socket, first_num, &first_token).await;
    assert_eq!(
        raw_answer(&mut r1).await["turn"],
        serde_json::json!("resumed")
    );
    let (mut r2, mut w2) = resume_raw(&socket, second_num, &second_token).await;
    assert_eq!(
        raw_answer(&mut r2).await["turn"],
        serde_json::json!("resumed")
    );
    w1.write_all(b"{\"step\":\"end\",\"outcome\":\"exited 0\"}\n")
        .await
        .expect("end 1");
    drop(w1);
    assert_eq!(raw_answer(&mut r2).await["turn"], serde_json::json!("held"));
    let third_moved = raw_answer(&mut third_r).await;
    assert_eq!(
        (&third_moved["turn"], &third_moved["ahead"]),
        (&serde_json::json!("queued"), &serde_json::json!(1)),
        "third must receive its updated queue position when first ends: {third_moved}"
    );
    w2.write_all(b"{\"step\":\"end\",\"outcome\":\"exited 0\"}\n")
        .await
        .expect("end 2");
    drop(w2);
    assert_eq!(
        raw_answer(&mut third_r).await["turn"],
        serde_json::json!("held")
    );
    drop(third_w);
    drop(third_r);
    new.drain().await.expect("drain new");
}

/// Crash case 1: an asker dies (SIGKILL) during the restart gap while its bound command is still
/// running; the replacing gateway adopts the ticket with `asker = false`, re-arms `ExitWatch` on
/// the bound command, keeps the turn held until that command exits, then records the ticket
/// abandoned and promotes the next waiter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_asker_that_dies_during_the_restart_gap_keeps_its_bound_command_holding_the_turn_until_it_exits()
 {
    let (old, root, socket) = gateway().await;
    let cache = root.path().join("cache");

    let mut asker = spawn_remote_asker(&socket, "doomed-asker", true);
    let mut asker_out = BufReader::new(asker.stdout.take().expect("asker stdout")).lines();
    let mut asker_in = asker.stdin.take().expect("asker stdin");

    let held = raw_answer(&mut asker_out).await;
    assert_eq!(held["turn"], serde_json::json!("held"), "{held}");
    let spawned = raw_answer(&mut asker_out).await;
    let bound_pid = spawned["spawnedPid"].as_u64().expect("bound child pid") as i32;
    let bound = raw_answer(&mut asker_out).await;
    assert_eq!(bound["turn"], serde_json::json!("bound"), "{bound}");

    let mut install = waiting_fence(&socket).await;
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(false)
    );
    assert_eq!(
        raw_answer(&mut asker_out).await["turn"],
        serde_json::json!("fence")
    );
    asker_in
        .write_all(b"{\"step\":\"fenced\"}\n")
        .await
        .expect("ack fence");
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(true)
    );

    // Drain old gateway, then SIGKILL the asker during the gap while its bound child lives on.
    old.drain().await.expect("drain old");
    drop(install);
    asker.start_kill().expect("SIGKILL asker during gap");
    let _ = asker.wait_bounded().await;

    let new = gateway_at(&socket, &cache).await;
    assert_eq!(
        tickets(&socket).await,
        ["doomed-asker"],
        "bound child must still hold the turn after asker died in the gap"
    );
    let (mut next_r, _next_w) = ask_resumable_raw(&socket, REPO, "next").await;
    assert_eq!(
        raw_answer(&mut next_r).await["turn"],
        serde_json::json!("queued"),
        "next waiter must queue behind the adopted bound command"
    );

    // Killing the bound child fires the re-armed ExitWatch and promotes `next`.
    // SAFETY: `bound_pid` is the detached sleep child spawned by our test helper.
    unsafe {
        libc::kill(bound_pid, libc::SIGKILL);
    }
    let next_held = raw_answer(&mut next_r).await;
    assert_eq!(
        next_held["turn"],
        serde_json::json!("held"),
        "re-armed ExitWatch must promote next waiter when bound command exits: {next_held}"
    );
    let q = queue(&socket).await.expect("queue");
    assert!(
        q.abandoned
            .iter()
            .any(|gone| gone.ticket.workspace == "doomed-asker"),
        "asker that died in the gap must be recorded abandoned once its bound command exits: {q:?}"
    );
    new.drain().await.expect("drain new");
}

/// Crash case 2: an asker with no bound command dies (SIGKILL) during the restart gap; the
/// replacing gateway detects it gone at startup adoption, abandons the ticket immediately, and
/// promotes the next waiter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_asker_with_no_bound_command_that_dies_during_the_restart_gap_frees_the_turn_at_adoption()
 {
    let (old, root, socket) = gateway().await;
    let cache = root.path().join("cache");

    let mut asker = spawn_remote_asker(&socket, "gone-alone", false);
    let mut asker_out = BufReader::new(asker.stdout.take().expect("asker stdout")).lines();
    let mut asker_in = asker.stdin.take().expect("asker stdin");
    assert_eq!(
        raw_answer(&mut asker_out).await["turn"],
        serde_json::json!("held")
    );

    let (mut waiter_r, mut waiter_w) = ask_resumable_raw(&socket, REPO, "survivor").await;
    let queued = raw_answer(&mut waiter_r).await;
    assert_eq!(queued["turn"], serde_json::json!("queued"), "{queued}");
    let waiter_num = queued["ticket"]["number"].as_u64().expect("waiter num");
    let waiter_token = queued["resumeToken"]
        .as_str()
        .expect("waiter token")
        .to_owned();

    let mut install = waiting_fence(&socket).await;
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(false)
    );
    assert_eq!(
        raw_answer(&mut asker_out).await["turn"],
        serde_json::json!("fence")
    );
    assert_eq!(
        raw_answer(&mut waiter_r).await["turn"],
        serde_json::json!("fence")
    );
    asker_in
        .write_all(b"{\"step\":\"fenced\"}\n")
        .await
        .expect("fence asker");
    waiter_w
        .write_all(b"{\"step\":\"fenced\"}\n")
        .await
        .expect("fence waiter");
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(true)
    );

    old.drain().await.expect("drain old");
    drop(install);
    drop(waiter_r);
    drop(waiter_w);
    asker.start_kill().expect("SIGKILL asker during gap");
    let _ = asker.wait_bounded().await;

    let new = gateway_at(&socket, &cache).await;
    assert_eq!(
        tickets(&socket).await,
        ["survivor"],
        "gone asker with no bound command must be freed at adoption, promoting survivor"
    );
    let (mut resumed_r, _resumed_w) = resume_raw(&socket, waiter_num, &waiter_token).await;
    let resumed = raw_answer(&mut resumed_r).await;
    assert_eq!(
        resumed["turn"],
        serde_json::json!("held"),
        "promoted waiter resuming after holder died in gap must be told held: {resumed}"
    );
    new.drain().await.expect("drain new");
}

/// Crash case 3: a bound command exits during the restart gap while the asker stays alive; the
/// replacing gateway clears the exited command from `bound` on adoption (`ProcessExit::Before`),
/// and the asker resumes and finishes cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bound_command_that_exits_during_the_restart_gap_is_cleared_on_adoption_while_the_asker_resumes()
 {
    let (old, root, socket) = gateway().await;
    let cache = root.path().join("cache");

    let (mut land_r, mut land_w) = ask_resumable_raw(&socket, REPO, "asker-alive").await;
    let held = raw_answer(&mut land_r).await;
    assert_eq!(held["turn"], serde_json::json!("held"), "{held}");
    let ticket_num = held["ticket"]["number"].as_u64().expect("ticket num");
    let token = held["resumeToken"].as_str().expect("token").to_owned();

    let mut cmd = tokio::process::Command::new("sh");
    cmd.args(["-c", "exec cat >/dev/null"])
        .stdin(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut cmd = cmd.spawn().expect("spawn bound cmd");
    let cmd_pid = cmd.id().expect("cmd pid");
    land_w
        .write_all(format!("{{\"step\":\"bind\",\"pid\":{cmd_pid}}}\n").as_bytes())
        .await
        .expect("bind cmd");
    assert_eq!(
        raw_answer(&mut land_r).await["turn"],
        serde_json::json!("bound")
    );

    let mut install = waiting_fence(&socket).await;
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(false)
    );
    assert_eq!(
        raw_answer(&mut land_r).await["turn"],
        serde_json::json!("fence")
    );
    land_w
        .write_all(b"{\"step\":\"fenced\"}\n")
        .await
        .expect("ack fence");
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(true)
    );

    old.drain().await.expect("drain old");
    drop(install);
    drop(land_r);
    drop(land_w);

    // Bound command exits and is reaped during the gap.
    drop(cmd.stdin.take());
    assert!(
        cmd.wait_bounded()
            .await
            .expect("cmd exits in gap")
            .success()
    );

    let new = gateway_at(&socket, &cache).await;
    let adopted = queue(&socket).await.expect("queue adopted");
    assert_eq!(adopted.tickets.len(), 1);
    assert!(
        adopted.tickets[0].bound.is_empty(),
        "exited bound command must be cleared at adoption: {:?}",
        adopted.tickets[0]
    );
    let (mut resumed_r, mut resumed_w) = resume_raw(&socket, ticket_num, &token).await;
    assert_eq!(
        raw_answer(&mut resumed_r).await["turn"],
        serde_json::json!("resumed")
    );
    resumed_w
        .write_all(b"{\"step\":\"end\",\"outcome\":\"exited 0\"}\n")
        .await
        .expect("end resumed");
    drop(resumed_w);
    drop(resumed_r);
    new.drain().await.expect("drain new");
}

/// Crash case 4: the asker sent `end` before drain (so only its bound command held the ticket,
/// already fenced), and the bound command exits during the restart gap; the replacing gateway
/// finishes the ticket on adoption without abandoning it and promotes the next waiter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bound_command_that_exits_during_the_restart_gap_after_asker_ended_finishes_the_ticket_on_adoption()
 {
    let (old, root, socket) = gateway().await;
    let cache = root.path().join("cache");

    let (mut land_r, mut land_w) = ask_resumable_raw(&socket, REPO, "ended-holder").await;
    let held = raw_answer(&mut land_r).await;
    assert_eq!(held["turn"], serde_json::json!("held"), "{held}");

    let mut cmd = tokio::process::Command::new("sh");
    cmd.args(["-c", "exec cat >/dev/null"])
        .stdin(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut cmd = cmd.spawn().expect("spawn bound cmd");
    let cmd_pid = cmd.id().expect("cmd pid");
    land_w
        .write_all(format!("{{\"step\":\"bind\",\"pid\":{cmd_pid}}}\n").as_bytes())
        .await
        .expect("bind cmd");
    assert_eq!(
        raw_answer(&mut land_r).await["turn"],
        serde_json::json!("bound")
    );

    // Asker ends its hold while bound command still runs: ticket has no live asker, so it is
    // already fenced.
    land_w
        .write_all(b"{\"step\":\"end\",\"outcome\":\"exited 0\"}\n")
        .await
        .expect("end asker");
    drop(land_w);
    drop(land_r);

    // Waiting install must fence at once since the only live ticket has no live asker.
    let mut install = waiting_fence(&socket).await;
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(true),
        "ticket with departed asker is already fenced"
    );

    old.drain().await.expect("drain old");
    drop(install);

    // Bound command exits during the gap.
    drop(cmd.stdin.take());
    assert!(
        cmd.wait_bounded()
            .await
            .expect("cmd exits in gap")
            .success()
    );

    let new = gateway_at(&socket, &cache).await;
    assert_eq!(
        tickets(&socket).await,
        Vec::<String>::new(),
        "ticket whose asker ended and whose bound command exited in the gap must finish at adoption"
    );
    assert!(
        queue(&socket).await.is_none_or(|q| q.abandoned.is_empty()),
        "finished ticket must not be recorded abandoned"
    );
    new.drain().await.expect("drain new");
}

/// Crash case 5: a replacing gateway that lacks the handoff snapshot (or receives a wrong token)
/// refuses `land-turn-resume`, leaving the caller on the lost-turn path (05_gateway.md "Land
/// turns").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_gateway_lacking_the_resume_token_refuses_resume_and_triggers_lost_turn() {
    let (old, root, socket) = gateway().await;
    let cache = root.path().join("cache");

    // 1. Fresh gateway with no adopted tickets refuses `land-turn-resume` (and recognizes the op,
    //    rather than answering "unknown gateway control operation").
    let (mut missing_r, _missing_w) = resume_raw(&socket, 99, "missing-token").await;
    let refused = raw_answer(&mut missing_r).await;
    assert_eq!(
        refused["turn"],
        serde_json::json!("refused"),
        "missing ticket must be refused as a turn answer, not an unknown control op: {refused}"
    );
    drop(missing_r);

    // 2. Adopted ticket refuses a wrong token without destroying the ticket for the real token.
    let (mut land_r, mut land_w) = ask_resumable_raw(&socket, REPO, "token-checked").await;
    let held = raw_answer(&mut land_r).await;
    assert_eq!(held["turn"], serde_json::json!("held"), "{held}");
    let ticket_num = held["ticket"]["number"].as_u64().expect("ticket num");
    let token = held["resumeToken"].as_str().expect("token").to_owned();

    let mut install = waiting_fence(&socket).await;
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(false)
    );
    assert_eq!(
        raw_answer(&mut land_r).await["turn"],
        serde_json::json!("fence")
    );
    land_w
        .write_all(b"{\"step\":\"fenced\"}\n")
        .await
        .expect("ack fence");
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(true)
    );

    old.drain().await.expect("drain old");
    drop(install);
    drop(land_r);
    drop(land_w);

    let new = gateway_at(&socket, &cache).await;
    let (mut bad_r, _bad_w) = resume_raw(&socket, ticket_num, "definitely-not-the-token").await;
    let bad = raw_answer(&mut bad_r).await;
    assert_eq!(
        bad["turn"],
        serde_json::json!("refused"),
        "wrong resume token must be refused: {bad}"
    );
    drop(bad_r);

    let (mut ok_r, mut ok_w) = resume_raw(&socket, ticket_num, &token).await;
    assert_eq!(
        raw_answer(&mut ok_r).await["turn"],
        serde_json::json!("resumed"),
        "real token must still resume after a bad token attempt"
    );
    ok_w.write_all(b"{\"step\":\"end\",\"outcome\":\"exited 0\"}\n")
        .await
        .expect("end");
    drop(ok_w);
    drop(ok_r);
    new.drain().await.expect("drain new");
}

/// Legacy compatibility (05_gateway.md "The read fences the turns", "Land turns"): an ask that
/// omits `"resumable"` is never sent `{"turn":"fence"}` (an older client treats any unexpected
/// line on a held turn as a lost turn and signals its gate), and the waiting install still waits
/// for the legacy ticket to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_legacy_non_resumable_turn_is_never_sent_fence_and_still_waits_to_end() {
    let (old, _root, socket) = gateway().await;

    // Raw ask omitting `"resumable"`.
    let mut legacy = UnixStream::connect_bounded(&socket)
        .await
        .expect("connect legacy ask");
    let ask = serde_json::json!({
        "op": land_turn::LAND_TURN_OP,
        "repo": REPO,
        "workspace": "legacy",
        "command": "land legacy",
    });
    legacy
        .write_all(format!("{ask}\n").as_bytes())
        .await
        .expect("send legacy ask");
    let (legacy_r, mut legacy_w) = legacy.into_split();
    let mut legacy_answers = BufReader::new(legacy_r).lines();
    let held = raw_answer(&mut legacy_answers).await;
    assert_eq!(held["turn"], serde_json::json!("held"), "{held}");
    assert!(
        held.get("resumeToken").is_none(),
        "legacy non-resumable ask must not receive a resumeToken: {held}"
    );

    // Waiting install begins waiting: the actor has now processed `Wait` and would have sent
    // `{"turn":"fence"}` to any resumable ticket.
    let mut install = waiting_fence(&socket).await;
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(false)
    );

    // Sending a bind on the legacy connection proves no `{"turn":"fence"}` was queued ahead of
    // its `{"turn":"bound"}` answer, and the bind still succeeds.
    let mut cmd = tokio::process::Command::new("sh");
    cmd.args(["-c", "exec cat >/dev/null"])
        .stdin(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut cmd = cmd.spawn().expect("spawn legacy cmd");
    let cmd_pid = cmd.id().expect("cmd pid");
    legacy_w
        .write_all(format!("{{\"step\":\"bind\",\"pid\":{cmd_pid}}}\n").as_bytes())
        .await
        .expect("bind legacy cmd");
    let next = raw_answer(&mut legacy_answers).await;
    assert_eq!(
        next["turn"],
        serde_json::json!("bound"),
        "legacy ticket must never receive a fence notice before bound: {next}"
    );

    // Ending the legacy ticket and its bound command fences the waiting install.
    legacy_w
        .write_all(b"{\"step\":\"end\",\"outcome\":\"exited 0\"}\n")
        .await
        .expect("end legacy");
    drop(legacy_w);
    drop(cmd.stdin.take());
    assert!(cmd.wait_bounded().await.expect("legacy cmd ends").success());
    assert_eq!(
        control_line(&mut install).await["fenced"],
        serde_json::json!(true),
        "install fences once the legacy ticket ends"
    );
    old.drain().await.expect("drain old");
}
